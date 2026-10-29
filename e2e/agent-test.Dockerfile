# Test agent image: the claude-code-wrapper image driven by the wrapper's scripted fake claude.
# There is no derived production agent image (D16); this one exists for the contract suite
# (tests/fakes/real_wrapper.py) and kind-e2e. Build from the repo root:
#   docker build -f e2e/agent-test.Dockerfile -t agents-orchestrator-agent-test:local .
# Run it with e2e/agent-test.config.yaml mounted at /etc/ccw/config.yaml.
ARG BASE_IMAGE=harbor.szymonrichert.pl/containers/claude-code-wrapper:cdc786a
# Wrapper commit whose tests/fake_claude is copied; keep it equal to the BASE_IMAGE sha.
ARG WRAPPER_SHA=cdc786ac45ece51edc2300907c9579b2ed9fdc4b

FROM ${BASE_IMAGE}
ARG WRAPPER_SHA
USER root
ADD --chmod=0755 https://raw.githubusercontent.com/szymonrychu/claude-code-wrapper/${WRAPPER_SHA}/tests/fake_claude/fake_claude.py /opt/fake-claude/fake_claude.py
RUN mkdir -p /opt/agent-test && chmod 0755 /opt/agent-test
COPY --chmod=0644 e2e/agent-test.scenario.json /opt/agent-test/scenario.json
RUN printf '#!/bin/sh\nexec /usr/local/bin/python -I /opt/fake-claude/fake_claude.py "$@"\n' > /opt/agent-test/claude \
 && chmod 0755 /opt/agent-test/claude
USER agent
