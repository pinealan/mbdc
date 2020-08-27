(ns metabase.driver.datomic-client-test
  (:require [clojure.test :refer :all]
            [matcher-combinators.test :refer :all]
            [matcher-combinators.matchers :as m]

            [clojure.java.io :as jio]
            [datomic.client.api :as dc]
            [datomic.dev-local :as dl])
  (:require
   [metabase.driver :as driver]
   [metabase.driver.datomic-client :refer :all]))

(defn conj-tx [with-db tx]
  (-> with-db
      (dc/with {:tx-data tx})
      :db-after))

(def test-db-spec
  {:server-type :dev-local
   :storage-dir "/tmp/datomic-dev-local"
   :system      "test-mb-system"
   :db-name     "test-db"})

(defn test-db []
  (let [db-spec test-db-spec
        _ (.mkdirs (jio/file (:storage-dir db-spec)))
        client (dc/client db-spec)]
    (dc/delete-database client db-spec)
    (dc/create-database client db-spec)
    (-> client
        (dc/connect db-spec)
        (dc/with-db)
        (conj-tx []))))

(defn db-of
  "Returns an empty dev-local DB with no schema."
  [& txs]
  (reduce conj-tx (test-db) txs))

(defn simple-schema
  "Helper to create transactable schema entities.
   Also used as result from `schema-attrs-q`."
  [idents]
  (for [[ident vtype] idents]
    {:db/ident ident
     :db/valueType vtype
     :db/cardinality :db.cardinality/one}))

(deftest can-connect?-test
  (let [_ (test-db)]
    (is (driver/can-connect? :datomic-client test-db-spec))))

(deftest derive-table-names-test
  (testing "infer 1 table from schema"
    (let [schema (simple-schema {:table/a1 :db.type/ref
                                 :table/a2 :db.type/ref})]
      (is (match?
           (m/in-any-order ["table"])
           (derive-table-names schema)))))

  (testing "infer 2 table from schema"
    (is (match?
         (m/in-any-order ["table-1", "table-2"])
         (-> {:table-1/a :db.type/ref
              :table-1/b :db.type/ref
              :table-2/a :db.type/ref}
             (simple-schema)
             (derive-table-names))))))

(deftest describe-table-test
  (testing "infer attributes with type from given table"
    (is (match?
         {:table-1/a :db.type/ref
          :table-1/b :db.type/long}
         (table-columns "table-1"
                        (simple-schema
                         {:table-1/a :db.type/ref
                          :table-1/b :db.type/long
                          :table-2/a :db.type/string}))))))

(deftest guess-column-dest-test
  (testing "finds frequent attr namespace pointed to by ref attribute"
    (let [db (-> {:table-1/a :db.type/ref
                  :table-1/b :db.type/long
                  :table-2/a :db.type/string}
                 (simple-schema)
                 (db-of [{:table-1/a "some table-2"
                          :table-1/b 1}
                         {:db/id     "some table-2"
                          :table-2/a "val"}]))]
      (is (= "table-2"
             (guess-column-dest db "table-1" :table-1/a))))))

(comment
  ;; cleanup-db
  (dc/delete-database (dc/client test-db-spec) test-db-spec)
  (dl/release-db test-db-spec))
