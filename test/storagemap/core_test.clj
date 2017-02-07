(ns storagemap.core-test
  (:require [clojure.test :refer :all]
            [storagemap.core :refer :all])
  (:import [storagemap.core NopSerializer]))

(defmacro def-storage-test [name prefix & code]
  `(deftest ~name
     (let [~'j (atom {})
           ~'rm (storage-map ~'j ~prefix (NopSerializer.))]
         (reset! ~'j {})
         ~@code
         )))

(defn equivalence [expected actual]
  (is (= expected actual))
  (is (= actual expected))
  (is (= actual actual)))


(def-storage-test assoc-test "s"
  (swap! j assoc "s:a" {"a" 1, "b" 2})
  (swap! j assoc "s:b" {"x" 1, "y" 2})
  (let [assoc-rm (assoc rm "c" 10)]
    (is (equivalence {"b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} rm))
    (is (equivalence {"c" 10, "b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} assoc-rm))))
    

#_(def-storage-test dissoc-test "s"
   (swap! j assoc "s:a" {"a" 1, "b" 2})
   (swap! j assoc "s:b" {"x" 1, "y" 2})
   (let [dissoc-rm (dissoc rm "a")]
     (is (equivalence {"b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} rm))    
     (is (equivalence {"b" {"x" 1, "y" 2}} dissoc-rm))))


#_(def-storage-test assoc-dissoc-test "s"
   (swap! j assoc "s:a" {"a" 1, "b" 2})
   (swap! j assoc "s:b" {"x" 1, "y" 2})
   (let [assoc-rm (assoc rm "c" 10)
         dissoc-rm (dissoc rm "c")]
     (is (equivalence {"b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} rm))
     (is (equivalence {"c" 10, "b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} assoc-rm))
     (is (equivalence {"b" {"x" 1, "y" 2}, "a" {"a" 1, "b" 2}} dissoc-rm))))


#_(def-storage-test persistence-test "s"
   (swap! j assoc "s:a" {"a" 1, "b" 2})
   (swap! j assoc "s:b" {"x" 1, "y" 2})
   (let [assoc-rm (assoc rm "c" 10)
         dissoc-rm (dissoc rm "b")]
     (store! dissoc-rm)
     (is (equivalence {"a" {"a" 1, "b" 2}} rm))
     (is (equivalence {"a" {"a" 1, "b" 2}} (storage-map (atom {}) "s" (NopSerializer.))))
     ))
