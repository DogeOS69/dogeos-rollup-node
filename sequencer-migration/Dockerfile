FROM ghcr.io/foundry-rs/foundry:stable

USER root
RUN apt-get update && apt-get install -y \
    jq

WORKDIR /migration
COPY . .

RUN chmod +x switch-to-l2reth.sh \
           send-l1-messages.sh

# Set default environment variables (can be overridden at runtime)
ENV L2GETH_RPC_URL=http://host.docker.internal:8547
ENV L2RETH_RPC_URL=http://host.docker.internal:8545

USER foundry

# Auto-source common functions when bash starts
RUN echo 'source /migration/common-functions.sh' >> /home/foundry/.bashrc

# Default to interactive bash shell with RC file loaded
CMD ["/bin/bash", "-l"]