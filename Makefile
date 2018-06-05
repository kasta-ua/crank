.PHONY: test

test:
	clojure -C:test -R:test -m crank.test

test-repl:
	clj -C:test -R:test
