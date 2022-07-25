.PHONY: test

test:
	clojure -C:test -R:test -m crank.test

test-repl:
	clj -C:test -R:test

nrepl:
	clj -A:test:repl

deploy:
	lein deploy clojars
