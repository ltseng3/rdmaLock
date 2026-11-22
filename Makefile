.PHONY: test apt-build apt-check clean

BUILDDIR := mucql/build-local

test:
	cmake -S mucql -B $(BUILDDIR) -DCMAKE_BUILD_TYPE=RelWithDebInfo -DMUCQL_MOCK_VERBS_TEST=ON
	cmake --build $(BUILDDIR) --parallel 2
	ctest --test-dir $(BUILDDIR) --output-on-failure
	MUCQL_BUILD_DIR=$(CURDIR)/$(BUILDDIR) bash tools/test_tcp_quorum.sh
	python3 -m unittest discover -s tools/apt/tests -v

apt-build:
	bash tools/apt/build.sh

apt-check:
	bash tools/apt/check.sh $${IB_DEV:-mlx4_0} $${IB_PORT:-1}

clean:
	rm -rf $(BUILDDIR)
