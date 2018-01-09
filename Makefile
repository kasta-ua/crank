.PHONY: test

test:
	clojure -C:test -R:test -m crank.test
