PRETTIER = pnpx prettier

.PHONY: fmt-md
fmt-md:
	$(PRETTIER) README.md CHANGELOG.md --write
	
.PHONY: fmt-ci
fmt-ci:
	$(PRETTIER) .github --write