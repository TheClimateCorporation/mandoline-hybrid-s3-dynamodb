(ns io.mandoline.backend.hybrid-s3-dynamodb
  (:require
    [clojure.string :as string]
    [clojure.tools.logging :as log]
    [io.mandoline.utils :as utils]
    [io.mandoline.impl.protocol :as proto]
    [io.mandoline.backend.dynamodb :as ddback]
    [io.mandoline.backend.s3 :as s3back])
  (:import
    [com.amazonaws.services.dynamodbv2.model
     ResourceNotFoundException]))

(defn get-table-name
  "Construct a table name from one or more table name components.
  Keywords are automatically converted to their string names.

   (get-table-name \"com.foo.dev.bar\" \"datasetname\"))
    => \"com.foo.dev.bar.datasetname\"

   (get-table-name \"com.foo.dev.bar\" :datasetname))
  => \"com.foo.dev.bar.datasetname\"
  "
  [head & more]
  (string/join \. (map name (cons head more))))

; This somewhat unusual syntax is because we want to behave like a deftype, but
; delegate most of our actions to a DynamoDBConnection.
(defn ->HybridConnection [table s3-bucket s3-key-prefix dataset client-opts]
  (let [dynamo-conn (ddback/->DynamoDBConnection table client-opts)]
    (reify proto/Connection
      (index [_ var-name metadata options]
        (.index dynamo-conn var-name metadata options))

      (write-version [_ metadata]
        (.write-version dynamo-conn metadata))

      ; the only nondelegated method
      (chunk-store [_ options]
        (s3back/->S3ChunkStore s3-bucket s3-key-prefix dataset client-opts))

      (get-stats [_]
        {:metadata-size (ddback/table-stats client-opts
                                            (get-table-name table "versions"))
         :index-size (ddback/table-stats client-opts
                                         (get-table-name table "indices"))
         :data-size-unavailable true})

      (metadata [_ version]
        (.metadata dynamo-conn version))

      (versions [_ opts]
        (.versions dynamo-conn opts)))))

(defn ->HybridSchema [root-table s3-bucket s3-key-prefix client-opts]
  (let [dynamo-schema (ddback/->DynamoDBSchema root-table client-opts)]
    (reify proto/Schema
      (create-dataset [_ name]
        ; Only Dynamo tables need setup. The S3 chunk store starts without setup.
        (when-not (and (string? name) (not (string/blank? name)))
          (throw
            (IllegalArgumentException.
              "dataset name must be a non-empty string")))
        (let [root-path (get-table-name root-table name)]
          ; create-table throws ResourceInUseException if table already
          ; exists
          (ddback/create-table
            client-opts
            (ddback/get-table-name root-path "indices")
            [:k :s]
            :range-keydef [:c :n]
            :throughput (:indices ddback/*default-throughputs*))
          (ddback/create-table
            client-opts
            (ddback/get-table-name root-path "versions")
            [:k :s]
            :range-keydef [:t :n]
            :throughput (:versions ddback/*default-throughputs*)))
        nil)

      (destroy-dataset [_ name]
        (doseq [t ["versions" "indices"]]
          (->> (ddback/get-table-name root-table name t)
               (ddback/delete-table client-opts))))

      (list-datasets [_]
        (.list-datasets dynamo-schema))

      (connect [_ dataset-name]
        (let [conn (->HybridConnection (get-table-name root-table dataset-name)
                                       s3-bucket s3-key-prefix dataset-name client-opts)]
          (try ; Check that the connection is functional
            (.get-stats conn)
            (catch Exception e
              (throw
                (RuntimeException.
                  (format
                    "Failed to connect to dataset \"%s\" with root-table \"%s\""
                    dataset-name root-table)
                  e))))
          conn)))))


(defn mk-schema [store-spec]
  "Given a store spec map, return a HybridSchema instance

  The store-spec argument is a map that can include the following entries

  :root            - the DynamoDB root
  :s3-bucket       - the S3 bucket to use for storage
  :s3-key-prefix   - the S3 subdirectory to use for storage
  :db-version      - (optional) the version of this library
  :provider        - (optional) AWSCredentialsProvider to use
  :role-arn        - (optional) the IAM role to assume when accessing DynamoDB
  :session-name    - (optional) a unique identifier to id this role session
  :endpoint        - (optional) override endpoint URL for DynamoDB
  :conn-timeout    - (optional) timeout when opening connection
  :max-conns       - (optional) max number of HTTP connections
  :max-error-retry - (optional) max number of retries to attempt on recoverable failures
  :socket-timeout  - (optional) timeout for data transfer on an open connection

  For more details, see the docstring for the
  io.mandoline.backend.dynamodb/store-spec->client-opts function."
  (let [root-table (ddback/root-table-prefix
                     (:root store-spec) (:db-version store-spec))]
    (when-not (every? identity [(:s3-bucket store-spec) (:s3-key-prefix store-spec)])
      (throw (IllegalArgumentException. "must supply :root, :s3-bucket, :s3-key-prefix")))
    (->HybridSchema root-table (:s3-bucket store-spec) (:s3-key-prefix store-spec)
                    (ddback/store-spec->client-opts store-spec))))
