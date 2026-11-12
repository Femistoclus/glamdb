The `node-exporter.json` dashboard is rendered from `nodes.yaml` in
kube-prometheus-stack 92.7.0, Apache-2.0, with the Compose job selector changed
from `node-exporter` to `node`. The chart identifies its upstream source as
Prometheus Operator kube-prometheus commit
`f81c076ab8f542c457840cd4c62656b8d6b4f43a`,
`manifests/grafana-dashboardDefinitions.yaml` (Node Exporter Mixin).
The pinned chart also contains Kubernetes Mixin panels/rules; its dashboard
sync recipe pins `73ea726e63cc53c40e0412da02ea3da474f52686`.

Source and archive checksums are in `../versions.lock.json`. The upstream
Apache-2.0 license is retained in this directory. C++ client licenses remain in
the unmodified `third_party/prometheus-cpp` submodule, including CivetWeb's
upstream license. The Python client is Apache-2.0 AND BSD-2-Clause.
