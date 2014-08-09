(ns io.mandoline.backend.hybrid-s3-dynamodb-test
  (:require
    [amazonica.aws.s3 :as aws-s3]
    [clojure.test :refer :all]
    [clojure.tools.logging :as log]
    [clojure.string :refer [split join blank?]]
    [environ.core :refer [env]]
    [taoensso.faraday :as far]
    [io.mandoline :as db]
    [io.mandoline.impl :as impl]
    [io.mandoline.impl.protocol :as proto]
    [io.mandoline.slice :as slice]
    [io.mandoline.backend.dynamodb :as ddback]
    [io.mandoline.backend.hybrid-s3-dynamodb :as hybrid]
    [io.mandoline.test.utils :refer [random-name
                                     with-and-without-caches
                                     with-temp-db]]
    [io.mandoline.test
     [entire-flow :refer [entire-flow]]
     [nan :refer [fill-double
                  fill-float
                  fill-short]]]
    [io.mandoline.test.protocol
     [chunk-store :as chunk-store]
     [schema :as schema]])
  (:import
    [java.nio ByteBuffer]
    [org.apache.commons.codec.digest DigestUtils]
    [com.amazonaws AmazonClientException]
    [com.amazonaws.auth
     AWSCredentialsProvider
     BasicAWSCredentials
     DefaultAWSCredentialsProviderChain]
    [com.amazonaws.retry PredefinedRetryPolicies]
    [com.amazonaws.services.dynamodbv2 AmazonDynamoDBClient]
    [com.amazonaws.services.dynamodbv2.model
     AttributeValue
     GetItemRequest
     GetItemResult
     LimitExceededException
     ResourceInUseException
     ResourceNotFoundException
     ProvisionedThroughputExceededException]
    [io.mandoline.slab Slab]))

(def ^:private test-root-base
  "integration-testing.mandoline.io")

(def test-hybrid-throughputs
  {:chunks {:read 100 :write 100}
   :indices {:read 50 :write 50}
   :versions {:read 4 :write 2}})

; Run each test with binding to reduced DynamoDB throughouts.
(use-fixtures :each
  (fn [f]
    (binding [ddback/*default-throughputs* test-hybrid-throughputs]
      (f))))

(defn- table-exists?
  [creds table]
  (far/describe-table creds table))

(def ^:private test-dataset-name "test-dataset")

(defn setup
  "Create a random store spec for testing the DynamoDB Doc Brown
  backend.

  This function is intended to be used with the matching teardown
  function."
  []
  (assert (not (blank? (env :mandoline-s3-test-path)))
          (str "S3 backend integration tests require a MANDOLINE_S3_TEST_PATH "
               "environment variable: bucket/test-prefix"))
  (let [rname (random-name)
        root (format "%s.%s" rname test-root-base)
        s3-root (split (env :mandoline-s3-test-path) #"/" 2)
        s3-bucket (first s3-root)
        s3-key-prefix (join "/" [(second s3-root) rname])
        dataset test-dataset-name]
    {:store "hybrid-s3-dynamodb"
     :root root
     :s3-bucket s3-bucket
     :s3-key-prefix s3-key-prefix
     :dataset dataset}))

(defn localize
  "This returns client-opts for faraday to use to connect to the local
  dynamodb.

  These client-opts contain empty credentials and a local :endpoint"
  [store-spec]
  {:endpoint "http://localhost:8000"
   :secret-key ""
   :access-key ""})

(defn teardown
  [store-spec]
  (let [s (hybrid/mk-schema store-spec)]
    (with-test-out
      (println
        "Please wait for post-test cleanup of DynamoDB tables."
        "This may take a while.")
      (try
        (.destroy-dataset s test-dataset-name)
        ; Swallow any exception after making a best effort
        (catch Exception e
          (println
            (format "Failed to destroy the dataset: %s %s" test-dataset-name e)))))))

(defmacro deftest* [test-name & body]
  "A macro to generate two deftests.

   One to run against integration dynamodb, marked with ^:integration,
   and the other to run against a local dynamodb, marked with ^:local"
  (let [old-meta (meta test-name)]
  `(do
     (deftest ~(vary-meta (symbol (str test-name "-local")) merge old-meta {:local true})
       (try
         (with-redefs [io.mandoline.backend.dynamodb/store-spec->client-opts localize]
           ~@body)
         (catch AmazonClientException e#
           (if (re-find #"Connection to http://localhost:8000 refused" (.getMessage e#))
             (do
              (binding [*out* *err*]
                (println
                 "Unable to connect the local instance of DynamoDB on http://localhost:8000\n"
                 "See the README.md for instructions on running these tests\n"))
               (is false))
             (throw e#)))))
     (deftest ~(vary-meta (symbol (str test-name "-integration")) merge old-meta {:integration true})
       ~@body))))

; This is part of the following test
(deftest* ^:some-metadata foo nil)

(deftest test-deftest*-metadata
  "A test to make sure the metadata is preserved when using deftest*"
  (is foo-local)
  (is foo-integration)
  (is (contains? (meta (var foo-local)) :some-metadata))
  (is (contains? (meta (var foo-integration)) :some-metadata)))

(deftest* test-dynamodb-schema-properties
  (let [store-specs (atom {}) ; map of Schema instance -> store-spec
        setup-schema (fn []
                       (let [store-spec (setup)
                             s (hybrid/mk-schema store-spec)]
                         (swap! store-specs assoc s store-spec)
                         s))
        teardown-schema (fn [s]
                          (let [store-spec (@store-specs s)]
                            (teardown store-spec)))
        num-datasets 2]
    (schema/test-schema-properties-single-threaded
      setup-schema teardown-schema num-datasets)
    (schema/test-schema-properties-multi-threaded
      setup-schema teardown-schema num-datasets)))

(deftest test-hybrid-mk-schema
  (testing
    "mk-schema function instantiates AWS credentials"
    (let [real-provider (DefaultAWSCredentialsProviderChain.)
          spy-state (atom {:get-credentials 0 :refresh 0})
          spy-provider (reify AWSCredentialsProvider
                         (getCredentials [this]
                           (swap! spy-state update-in [:get-credentials] inc)
                           (.getCredentials real-provider))
                         (refresh [this]
                           (swap! spy-state update-in [:refresh] inc)
                           (.refresh real-provider)))
          store-spec (assoc (setup) :provider spy-provider)]
      ; Create an empty dataset
      (.create-dataset (hybrid/mk-schema store-spec) (:dataset store-spec))
      ; Check that getCredentials method was called
      (is (pos? (:get-credentials @spy-state)))))
  (testing
    "mk-schema function passes client configuration entries from store spec"
    (let [store-spec (assoc (setup)
                            :conn-timeout 1234
                            :socket-timeout 8765
                            :max-error-retry 7
                            :foo :bar)
          schema (hybrid/mk-schema store-spec)
          dataset (:dataset store-spec)
          db-client (deref #'far/db-client)]
      (with-redefs [far/db-client (fn [client-opts]
                                    (is (= (dissoc store-spec :provider)
                                           (dissoc client-opts :provider))
                                        (str "mk-schema function preserves "
                                             "client configuration options"))
                                    (db-client client-opts))]
        ; Create an empty dataset
        (.create-dataset schema dataset)
        ; Read a chunk from this dataset
        (-> schema
          (.connect dataset)
          (.chunk-store {})
          (.read-chunk "nonexistentchunk"))))))

(deftest* hybrid-entire-flow
  (with-and-without-caches
    (entire-flow setup teardown)))
