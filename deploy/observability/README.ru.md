# Полный стек наблюдаемости одной командой

Пошаговая подготовка чистого сервера и точная команда запуска:
[QUICKSTART.ru.md](QUICKSTART.ru.md).

Скрипт запускает Prometheus, Grafana, Loki, Alloy, Alertmanager, Linux
node_exporter, официальный Podman exporter и два лёгких read-only cgroup
exporter. Он использует rootful Podman CLI; Python-зависимостей и отдельного
Compose нет. Существующие сервер БД, gateway, внешний HTTP-прокси и SSO не
создаются и не заменяются. Их настоящие метрики и каталог логов задаются во
входном JSON.

## Что нужно на сервере

- Linux x86_64 или arm64, Python 3.9+, установленный Podman 5.x,
  cgroup v2 с `cgroupManager=systemd`, systemd и утилита `findmnt`.
- Root для Podman, read-only привязок системных каталогов, владельцев новых
  данных и установки отдельного boot unit. Локальный Unix socket
  `/run/podman/podman.sock` должен быть активен:
  `sudo systemctl enable --now podman.socket`. TCP API не требуется.
- Доступ к реестрам образов Docker Hub, GHCR и Quay либо заранее загруженные
  образы с точными digest из `versions.lock.json`.
- Отдельная постоянно смонтированная файловая система **ext4 или XFS для
  Loki**, с UUID и ёмкостью не больше `loki.max_bytes`. В примере это
  `/srv/glamdb-loki`, максимум 4 GiB. Монтирование нужно сохранить в fstab или
  отдельном systemd mount unit. Допустим заранее подготовленный loop image.
  Скрипт ничего не форматирует. Корневой раздел, bind общего подкаталога,
  tmpfs и отсутствие mount отвергаются: retention не является дисковой квотой.
  Первый запуск принимает только пустой раздел, кроме `lost+found`.
- Настоящий C++ сервер с отдельным `/metrics` и журналом `glamdb.jsonl`.
  Укажите UID/GID владельца каталога: Alloy читает от этого пользователя.
  Скрипт не меняет владельца и SELinux-метки исходного каталога приложения.
- Практический стартовый ориентир: 4 CPU, 4 GiB RAM и отдельный запас диска
  под Prometheus/Grafana. Ограничения процессов и памяти заданы для каждого
  контейнера; это начальные пределы, а не подтверждённый production-бюджет.

Проверьте факты на целевом сервере:

```sh
sudo podman info --format=json
findmnt --mountpoint /srv/glamdb-loki -o TARGET,SOURCE,FSTYPE,UUID,FSROOT
```

## Настройка и единственная команда запуска

Скопируйте `config.example.json` в `/etc/glamdb-observability.json` и укажите
реальные URL, метки сервера, путь/UID/GID логов и подготовленный раздел Loki.
`gateway` и `edge` можно оставить `null`, если таких работающих компонентов
нет. URL могут быть `http` или `https`; логин/пароль и токены в URL запрещены.
Для защищённых метрик используйте `bearer_token_file` с абсолютным путём к
закрытому файлу 0600/0400. TLS использует системные доверенные CA.

Затем из корня checkout выполните:

```sh
sudo python3 scripts/observability.py up --config /etc/glamdb-observability.json
```

Это создаст reviewable конфигурацию в `<state_dir>/generated`, собственные
именованные тома и контейнеры, дождётся `/ready` Loki перед запуском Alloy,
запустит Grafana/Prometheus и установит отдельный boot unit. Исходный checkout
и JSON должны оставаться по тем же абсолютным путям: unit вызывает этот скрипт.

Все внутренние HTTP-порты слушают **127.0.0.1**. Контейнеры используют host
network, чтобы настоящий сервер мог сохранить loopback-only `/metrics`.
Экспортёры не публикуются в LAN. Адрес интерфейса Grafana тоже loopback по
умолчанию; `ui_bind` меняйте только как явное решение оператора.

Откройте интерфейс через SSH:

```sh
ssh -N -L 3000:127.0.0.1:3000 user@server
```

Grafana доступна на `http://127.0.0.1:3000`, пользователь `admin`. Случайный
пароль сохранён в `<state_dir>/secrets/grafana-password`, 0600 в закрытом
каталоге. Скрипт выводит путь, но не значение. Пароль и ключ шифрования
Grafana сохраняются при повторных запусках и `down`.
Если у существующей установки потерян файл пароля или ключа Grafana, `up`
останавливается с требованием восстановить исходный файл из резервной копии.
Новый случайный файл не исправил бы пароль в сохранённой БД и мог бы нарушить
расшифровку данных. Автоматической смены или сброса здесь нет.

Для предварительного просмотра без запуска контейнеров:

```sh
sudo python3 scripts/observability.py init --config /etc/glamdb-observability.json
```

Изменяйте исходный JSON или версионируемые шаблоны, затем снова запускайте
`up`. Одинаковый вход сохраняет ID контейнеров, тома, пароли и содержимое
данных. Изменение конфигурации сейчас пересоздаёт все девять собственных
контейнеров с сохранением томов; учитывайте короткое окно недоступности.
Имя установки, state path, mount path и предел диска Loki неизменяемы:
перенос данных требует отдельной явной процедуры.

## Состояние, фактическая проверка и остановка

```sh
sudo python3 scripts/observability.py status --config /etc/glamdb-observability.json
sudo python3 scripts/observability.py check --config /etc/glamdb-observability.json
sudo python3 scripts/observability.py down --config /etc/glamdb-observability.json
```

`check` проверяет работающие контейнеры, UUID/ёмкость mount, все scrape targets
и **ожидаемые реальные серии**, включая native readiness/operations,
gateway readiness/requests и правильную Caddy family с workbench/subroute.
Одного HTTP 200 или `up=1` недостаточно. Он сопоставляет настоящие последние
schema1 request ID/event/timestamp из ограниченного хвоста исходного файла с
записями Loki и показывает счётчики read/sent/drop/retry Alloy. Фальшивые
события, запросы БД и маркеры в журнал не записываются.
Старые TSDB-сэмплы не принимаются за новый scrape после рестарта: проверяются
observation timestamps, старт текущих контейнеров и Prometheus. Сразу после
`up` дождитесь первого 15-секундного scrape и повторите `check` при временном
NoData. Покрытие контейнеров сравнивается с текущим локальным `podman ps`.
Необязательные node-источники со `success=0` перечислены в `optional_no_data`;
они не заменяют успех обязательных CPU/memory/disk/filesystem/network collectors.
Если такой источник нужен именно этому серверу, проверьте его отдельно.

Пустой/невалидный исходный журнал или отсутствие доставки даёт FAIL, даже
когда reader активен. Записи старше 24 часов и отсутствие реального трафика
могут оставить проверку доставки без данных: создайте обычную разрешённую
операцию приложения и повторите проверку. Счётчик прочитанных байтов —
checkpoint gauge, он не является точным backlog.

Для стандартных прямых Podman-контейнеров и Quadlet проверяется покрытие
каждого работающего контейнера реальным leaf cgroup. Вложенные произвольные
cgroup и `cgroupfs` manager не квалифицированы; missing fields остаются NoData.
Здесь нет Kubernetes-кластера и pod-parent идентичностей.

`down` останавливает/удаляет только контейнеры с совпадающим случайным
installation owner и выключает только собственный boot unit. Именованные
тома, данные Loki, пароли, конфигурация и mount сохраняются. Ресурс с тем же
именем без правильной owner-метки вызывает отказ **до остановки** других
ресурсов. `status`/`down` не требуют исходных файлов секретов после установки.

Loki намеренно имеет `restart=no`: автоматический перезапуск Podman не должен
обойти проверку mount и записать данные на underlying rootfs. После сбоя
Loki повторите `up`; тот же путь используется при boot и перед запуском
проверяет UUID, лимит и owner. У остальных контейнеров `unless-stopped`.
Если mount пропал/заменён, запуск завершается ошибкой, без fallback-записей.
Все контейнерные журналы ограничены штатным `k8s-file max-size=10mb`.

На SELinux enforcing собственная общая конфигурация получает shared `:z`,
секреты и собственный раздел Loki — private `:Z`. Read-only доступ Alloy к
исходным логам использует явную policy `container_runtime_t`, сохраняя метки
приложения. Podman exporter получает только локальный Unix socket: `:ro`
не ограничивает API внутри сокета, поэтому этот официальный collector остаётся
доверенным компонентом. API не открывается по TCP.

## Telegram

По умолчанию `telegram: null`: состояния/алерты видны в Prometheus/Grafana/
Alertmanager, внешняя доставка не настроена. Чтобы использовать штатный
Telegram receiver Alertmanager, задайте два входных файла 0600/0400:

```json
"telegram": {
  "bot_token_file": "/etc/glamdb-secrets/telegram-token",
  "chat_id_file": "/etc/glamdb-secrets/telegram-chat"
}
```

Второй файл содержит целочисленный chat ID. Значения копируются в закрытый
state path и передаются процессу через отдельные file mounts. Они не попадают
в argv, сгенерированную публичную конфигурацию и вывод скрипта. Сохраняется
актуальная штатная группировка Alertmanager по cluster/service с ограниченным
темпом уведомлений.

## Воспроизводимость и пределы доказательств

Все образы закреплены digest. Prometheus/Grafana/Loki/Alloy/Alertmanager/node
используют проверенные multiarch manifest. Podman и cgroup exporter имеют
отдельные immutable amd64/arm64 image pins. Для cgroup amd64 SHA256 вычислен
по официальному GHCR manifest, совпал с Docker-Content-Digest, и проверена
архитектура config blob; исходник — release 0.3.3 commit9d9967a.

Генерация, конкуренция, сохранение файлов/секретов, отказы при небезопасных
URL/путях/ресурсах и проверка genuine log correlation покрыты:

```sh
python3 -m unittest discover -s tools/observability/tests -v
```

Полный opt-in lifecycle test требует отдельной подготовленной Linux-установки
с именем `glamdb-lab-*` и настоящими входными метриками/логами. Он выполняет
реальные up/check/repeated-up/down/up и в конце останавливает только свои
контейнеры, сохраняя данные:

```sh
sudo env GLAMDB_OBSERVABILITY_LIVE_CONFIG=/path/to/disposable-lab.json \
  python3 -m unittest discover -s tools/observability/tests -p test_live.py -v
```

Linux runtime qualification записывается отдельно в
`artifacts/observability/`; generation tests не являются доказательством
запуска другой CPU-архитектуры. Retention Prometheus 2 GB/7d ограничивает
retained blocks, но WAL/служебные файлы требуют дополнительного свободного
места; hard disk boundary в этом профиле отдельно доказана только для Loki.
Сохранение данных после orderly down/up не квалифицирует power-loss durability
движка БД, неизменность внешнего mount силами администратора или любые SLO.
