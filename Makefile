.PHONY: test

test:
	clojure -M:test -m crank.test

test-repl:
	clj -M:test

nrepl:
	clj -M:test:repl

deploy:
	lein deploy clojars
