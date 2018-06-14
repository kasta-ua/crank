.PHONY: test

test:
	clojure -C:test -R:test -m crank.test

test-repl:
	clj -C:test -R:test

nrepl:
	clj -C:test -R:test:repl -e '(require (quote cider-nrepl.main)) (cider-nrepl.main/init ["cider.nrepl/cider-middleware"])'
