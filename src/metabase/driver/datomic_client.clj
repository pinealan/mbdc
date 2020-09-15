(ns metabase.driver.datomic-client
  (:require [datomic.client.api :as dc]
            [datomic.client.api.async :as ds]
            [clojure.core.async :as a :refer [<! >! <!! >!! go close!]]
            [metabase.driver :as driver]
            [metabase.query-processor
             [reducible :as qp.reducible]
             [store :as qp.store]]))

(driver/register! :datomic-client)

(def features
  {:basic-aggregations                     true
   :standard-deviation-aggregations        true
   :case-sensitivity-string-filter-options true
   :foreign-keys                           true
   :nested-queries                         true
   :expressions                            false
   :expression-aggregations                false
   :native-parameters                      false
   :nested-fields                          false
   :left-join                              false
   :right-join                             false
   :inner-join                             false
   :full-join                              false
   :set-timezone                           false
   :binning                                false})

(defn latest-db
  ([] (latest-db (get (qp.store/database) :details)))
  ([db-spec]
   ;; merge default with provided spec so client can override server-type
   (-> {:server-type :peer-server
        :validate-hostnames false}
       (merge db-spec)
       (dc/client)
       (dc/connect db-spec)
       (dc/db))))

(defn kw->str [s]
  (str (namespace s) "/" (name s)))

(doseq [[feature supported?] features]
  (defmethod driver/supports? [:datomic-client feature] [_ _] supported?))

(defmethod driver/can-connect? :datomic-client [_ db-spec]
  (try
    (latest-db db-spec)
    true
    (catch Exception e
      false)))

(def datomic->metabase-type
  {:db.type/keyword :type/Keyword
   :db.type/string  :type/Text
   :db.type/boolean :type/Boolean
   :db.type/long    :type/Integer
   :db.type/bigint  :type/BigInteger
   :db.type/float   :type/Float
   :db.type/double  :type/Float
   :db.type/bigdec  :type/Decimal
   :db.type/ref     :type/FK
   :db.type/instant :type/DateTime
   :db.type/uuid    :type/UUID
   :db.type/uri     :type/URL
   :db.type/bytes   :type/Array

   ;; TODO(alan) Unhandled types
   ;; :db.type/symbol
   ;; :db.type/tuple
   ;; :db.type/uri    causes error on FE (Reader can't process tag object)
   })

(def reserved-prefixes
  #{"fressian"
    "db"
    "db.alter"
    "db.excise"
    "db.install"
    "db.sys"
    "db.attr"
    "db.entity"})

(def schema-attrs-q
  '{:find  [?attr ?type]
    :keys  [db/ident db/valueType]
    :where [[?schema-id :db/valueType ?type-id]
            [?schema-id :db/ident ?attr]
            [?type-id :db/ident ?type]]})

(defn attrs-by-table
  "Map from table name to collection of attribute entities."
  [attrs]
  (reduce #(update %1 (namespace (:db/ident %2)) conj %2) {} attrs))

(defn derive-table-names
  "Find all \"tables\" i.e. all namespace prefixes used in attribute names."
  [attrs]
  ;; TODO(alan) Use pattern match to remove all datomic reserved prefixes
  (remove reserved-prefixes
          (keys (attrs-by-table attrs))))

(defn table-columns
  "Given the name of a \"table\" (attribute namespace prefix), find all attribute
  names that occur in entities that have an attribute with this prefix."
  [table schema-attrs]
  (-> schema-attrs attrs-by-table (get table)
      (->> (map (juxt :db/ident :db/valueType))
           (into (sorted-map)))))

(defmethod driver/describe-database :datomic-client [_ instance]
  (let [db-spec (get instance :details)
        table-names (->> (latest-db db-spec)
                         (dc/qseq schema-attrs-q)
                         (flatten)
                         (derive-table-names))]
    {:tables
     (set
      (for [tname table-names]
        {:name   tname
         :schema nil}))}))

(defn column-name [table-name col-kw]
  (if (= (namespace col-kw)
         table-name)
    (name col-kw)
    (kw->str col-kw)))

(defmethod driver/describe-table :datomic-client
  [_ database {table-name :name}]
  (let [db          (latest-db (get database :details))
        schema      (dc/q schema-attrs-q db)
        cols        (table-columns table-name schema)]
    {:name   table-name
     :schema nil
     ;; Fields *must* be a set
     :fields
     (-> #{{:name          "db/id"
            :database-type "db.type/ref"
            :base-type     :type/PK
            :pk?           true}}
         (into (for [[col type] cols
                     :let [mb-type (datomic->metabase-type type)]
                     :when mb-type]
                 {:name          (column-name table-name col)
                  :database-type (kw->str type)
                  :base-type     mb-type
                  :special-type  mb-type})))}))

(defn guess-column-dest [db table-names col]
  (let [table? (into #{} table-names)
        attrs (-> {:find '[?ident]
                   :where [['_ col '?eid]
                           '[?eid ?attr]
                           '[?attr :db/ident ?ident]]}
                  (dc/q db)
                  (flatten))]
    (or (some->> attrs
                 (map namespace)
                 (remove #{"db"})
                 frequencies
                 (sort-by val)
                 last
                 key)
        (table? (name col)))))

(defmethod driver/describe-table-fks :datomic-client [_ database {table-name :name}]
  (let [db     (latest-db (get database :details))
        schema (dc/q schema-attrs-q db)
        tables (derive-table-names schema)
        cols   (table-columns table-name schema)]
    (-> #{}
        (into (for [[col type] cols
                    :when      (= type :db.type/ref)
                    :let       [dest (guess-column-dest db tables col)]
                    :when      dest]
                {:fk-column-name   (column-name table-name col)
                 :dest-table       {:name   dest
                                    :schema nil}
                 :dest-column-name "db/id"})))))

(defmethod driver/mbql->native :datomic-client
  [_ {mbqry :query settings :settings}]
  {:query "some query"})

(defmethod driver/execute-reducible-query :datomic-client
  [_ query context respond]
  (println (:native-query query))
  (respond
   {:cols [{:name "my_col"}]}
   (qp.reducible/reducible-rows (fn [& args] [1 "123"])
                                (:canceled-chan context))))

(comment
  (def lynx-spec
    {:endpoint "lynx.local:8998"
     :access-key "myaccesskey"
     :secret "mysecret"
     :db-name "m13n"})

  (def raven-spec
    {:endpoint "localhost:8998"
     :access-key "k"
     :secret "s"
     :db-name "m13n"})

  (require '[metabase.driver.datomic-client-test :refer [test-db-spec]])
  (def local-spec test-db-spec)

  (driver/can-connect? :datomic-client test-db-spec)

  (let [c (ds/q {:query schema-attrs-q
                 :args [(latest-db local-spec)]})]
    (prn (<!! c))
    (<!! c))

  (->> raven-spec
       (latest-db)
       (dc/q schema-attrs-q)
       (derive-table-names))
  (->> raven-spec
       (latest-db)
       (dc/q schema-attrs-q)
       (table-columns "txn"))

  (driver/describe-database
   :datomic-client
   {:details raven-spec})

  (driver/describe-table
   :datomic-client
   {:details raven-spec}
   {:name "operator"}))
