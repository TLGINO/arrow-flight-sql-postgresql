FROM mcr.microsoft.com/devcontainers/base:trixie

ARG POSTGRES_VERSION=17
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupmod --gid $USER_GID $USERNAME \
    && usermod --uid $USER_UID --gid $USER_GID $USERNAME \
    && chown -R $USER_UID:$USER_GID /home/$USERNAME

RUN apt-get update && apt-get upgrade -y \
    && sudo apt install -y -V ca-certificates lsb-release wget \
    && wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && sudo apt install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && rm ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    && sudo apt install -y postgresql-common \
    && sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y \
    && sudo apt-get install -y sudo postgresql-${POSTGRES_VERSION} postgresql-server-dev-${POSTGRES_VERSION} libkrb5-dev meson cmake ninja-build libarrow-flight-sql-dev libarrow-flight-sql-glib-dev ruby ruby-dev ruby-bundler libgirepository-1.0-dev \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    && echo 'export PATH="$(pg_config --bindir):$PATH"' > /home/vscode/.profile \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

USER ${USERNAME}
