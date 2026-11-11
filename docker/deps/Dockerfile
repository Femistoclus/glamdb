FROM glamdb-dev:clang18

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        libc++-18-dev libc++abi-18-dev libssl-dev make pkg-config strace zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

ENV CXXFLAGS=-stdlib=libc++

RUN printf '%s\n' '#include <expected>' '#include <format>' \
        'int main() { std::expected<int, int> value = 42; return std::format("{}", *value) != "42"; }' \
        | clang++-18 -std=c++23 -stdlib=libc++ -x c++ -o /tmp/cxx23-check - \
    && /tmp/cxx23-check \
    && rm /tmp/cxx23-check
