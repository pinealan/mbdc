(ns user
  (:require [clojure.java.io :as io]))

(defmacro jit
  "Just in time loading of dependencies."
  [sym]
  `(do
     (require '~(symbol (namespace sym)))
     (find-var '~sym)))

(defn plugin-yaml-path []
  (some #(when (.exists %) %)
        [(io/file "resources/metabase-plugin.yaml")
         (io/file "../mbdc/resources/metabase-plugin.yaml")]))

(defn setup-driver! []
  (-> (plugin-yaml-path)
      ((jit yaml.core/from-file))
      ((jit metabase.plugins.initialize/init-plugin-with-info!))))

(defn setup-db! []
  ((jit metabase.db/setup-db!)))

(defn open-metabase []
  ((jit clojure.java.browse/browse-url) "http://localhost:3000"))

(defn go []
  (let [start-web-server!   (jit metabase.server/start-web-server!)
        app                 (jit metabase.handler/app)
        init!               (jit metabase.core/init!)]
    #_(setup-db!)
    (start-web-server! app)
    (init!)
    (setup-driver!)
    (open-metabase)))

(defn refresh-all []
  ((jit clojure.tools.namespace.repl/set-refresh-dirs)
   "../metabase/src"
   "../metabase-datomic/src"
   "../metabase-datomic/dev"
   "../metabase-datomic/test")
  ((jit clojure.tools.namespace.repl/refresh-all)))

(comment
  (go)
  )

