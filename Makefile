PRETTIER = pnpx prettier

.PHONY: fmt-readme
fmt-readme:
	$(PRETTIER) README.md --write
	
.PHONY: fmt-ci
fmt-ci:
	$(PRETTIER) .github --write