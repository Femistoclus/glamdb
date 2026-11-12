# Подготовка нового Linux-сервера

Команды ниже выполняются **в SSH-сессии на целевом Linux-сервере**.
Сам запуск всего стека — одна команда `up`. Один раз перед ним нужны Podman,
конфигурация с реальными адресами и ограниченный диск для Loki.
Полные параметры и ограничения описаны в [README.ru.md](README.ru.md).

## 1. Установить необходимые программы

Нужны Python 3.9+, Podman 5.x, systemd, cgroup v2 и `findmnt`.
Для Fedora установка выглядит так:

```sh
sudo dnf install -y podman python3 util-linux e2fsprogs git nano
sudo systemctl enable --now podman.socket
sudo podman version
sudo podman info --format '{{.Host.CgroupsVersion}} {{.Host.CgroupManager}} {{.Host.Security.Rootless}}'
```

Последняя команда должна показать `v2 systemd false`. Для других дистрибутивов
используйте [официальную установку Podman](https://podman.io/docs/installation)
и проверьте версию пакета. Профиль rootful socket описан в
[официальной документации](https://docs.podman.io/en/latest/markdown/podman-system-service.1.html).
Подключение Podman к другому хосту для этого скрипта не подходит: он должен
проверять mount и запускать контейнеры на одном сервере.

Сохраните checkout GlamDB с этим скриптом по постоянному пути, например
`/opt/glamdb`. Этот путь понадобится boot unit. БД должна уже отдавать
настоящий private `/metrics`; её native-флаги:

```sh
glamdb --data-dir /path/to/database-service \
  --metrics-listen-address 127.0.0.1:9094 \
  --log-directory /var/log/glamdb
```

Это пример параметров существующей службы, а не команда для запуска второго
процесса над уже открытым каталогом БД. Сохраните её текущие параметры доступа,
порт, владельца и путь данных. Проверьте источник:

```sh
curl --fail --silent http://127.0.0.1:9094/metrics | head
stat -c 'uid=%u gid=%g' /var/log/glamdb/glamdb.jsonl
```

Запишите UID/GID: они нужны в конфиге Alloy. Если native-флаги отсутствуют,
нужна версия сервера с интеграцией мониторингов и структурированных логов.

## 2. Подготовить отдельное место для Loki

Если уже есть выделенный постоянно монтируемый ext4/XFS-раздел, используйте
его и переходите к настройке JSON. Для небольшого сервера можно создать
**новый** файл-образ на 4 GiB. Следующий блок не перезаписывает существующий
файл: `O_EXCL` останавливает его до форматирования. Он резервирует физическое
место и форматирует только что созданный обычный файл, не блочное устройство.

```sh
sudo python3 - <<'PY'
import os
from pathlib import Path
import subprocess

folder = Path('/var/lib/glamdb-observability-disks')
folder.mkdir(mode=0o700, parents=True, exist_ok=True)
image = folder / 'loki.ext4'
fd = os.open(image, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
try:
    os.posix_fallocate(fd, 0, 4 * 1024**3)
except BaseException:
    os.close(fd)
    image.unlink()
    raise
else:
    os.close(fd)
subprocess.run(['mkfs.ext4', '-F', str(image)], check=True)
PY
```

Если файл уже существует, остановитесь: не удаляйте и не форматируйте его
повторно. Если закончился диск, сначала подготовьте свободное место на сервере.
Повторный запуск стека не требует повторного создания образа.

Сохраните mount в отдельном systemd unit:

```sh
sudo mkdir -p /srv/glamdb-loki
glamdb_mount_unit=$(systemd-escape --path --suffix=mount /srv/glamdb-loki)
sudo tee "/etc/systemd/system/$glamdb_mount_unit" >/dev/null <<'UNIT'
[Unit]
Description=Dedicated bounded GlamDB Loki filesystem

[Mount]
What=/var/lib/glamdb-observability-disks/loki.ext4
Where=/srv/glamdb-loki
Type=ext4
Options=loop,nodev,nosuid,noexec

[Install]
WantedBy=multi-user.target
UNIT
sudo systemctl daemon-reload
sudo systemctl enable --now "$glamdb_mount_unit"
findmnt --mountpoint /srv/glamdb-loki -o TARGET,SOURCE,FSTYPE,UUID,FSROOT
df -h /srv/glamdb-loki
```

Ожидаются ext4, непустой UUID, FSROOT `/` и отдельная ёмкость около 4 GiB.
Не используйте общий корневой раздел или каталог приложения вместо этого тома.
Скрипт запомнит UUID и владельца; отсутствие или подмена mount заблокирует старт.

## 3. Заполнить конфиг

```sh
cd /opt/glamdb
sudo install -m 600 deploy/observability/config.example.json /etc/glamdb-observability.json
sudo nano /etc/glamdb-observability.json
```

В nano сохранение: `Ctrl+O`, затем `Enter`; выход: `Ctrl+X`.

Укажите:

- `cluster` и `node`: понятные метки вашей установки.
- `metrics.core.url`: адрес настоящих метрик native-сервера.
- `metrics.gateway` и `metrics.edge`: настоящие URL или `null`, если их нет.
- `logs.directory`, `logs.uid`, `logs.gid`: каталог и владелец исходного журнала.
- `loki.mount_path`: `/srv/glamdb-loki` в примере выше.
- `loki.max_bytes`: `4294967296` для образа 4 GiB.
- `state_dir`: постоянный каталог конфигурации и секретов, отдельный от mount.

`telegram: null` сохраняет алерты в интерфейсах без внешней отправки. Если
нужен существующий Telegram-бот, создайте входные файлы с закрытыми правами:

```sh
sudo python3 tools/alerting/configure.py --secrets-dir /etc/glamdb-telegram
sudo chmod 600 /etc/glamdb-telegram/telegram_bot_token /etc/glamdb-telegram/telegram_chat_id
```

Утилита спросит токен и chat ID. Инструкция получения бота и ID:
[ALERTING.md](../../ALERTING.md#configure-telegram-secrets).
В JSON замените `telegram: null` на:

```json
"telegram": {
  "bot_token_file": "/etc/glamdb-telegram/telegram_bot_token",
  "chat_id_file": "/etc/glamdb-telegram/telegram_chat_id"
}
```

Повторная настройка секретов требует явного `--rotate`; существующие файлы
утилита молча не перезаписывает. Не добавляйте значения токенов в Git или URL.

## 4. Запустить всё одной командой

```sh
sudo python3 scripts/observability.py up --config /etc/glamdb-observability.json
```

Проверить результат:

```sh
sudo python3 scripts/observability.py check --config /etc/glamdb-observability.json
```

Ожидается `result: PASS`. Проверка сверяет настоящие семейства метрик и
доставленные записи журнала. Если журнал пустой, выполните обычную разрешённую
операцию приложения, подождите несколько секунд и повторите `check`.

## 5. Открыть Grafana с ноутбука

В новом терминале **на ноутбуке**, заменив `user@server` своими данными:

```sh
ssh -N -L 3000:127.0.0.1:3000 user@server
```

Оставьте этот терминал открытым, откройте http://127.0.0.1:3000.
Если локальный порт 3000 уже занят, используйте `-L 33000:127.0.0.1:3000`
и откройте http://127.0.0.1:33000.
Логин — `admin`. Путь к случайному паролю напечатан скриптом; по умолчанию:

```sh
sudo cat /var/lib/glamdb-observability/server-1/secrets/grafana-password
```

Эту команду вводите на сервере. Не отправляйте её вывод в общий чат.
Выберите папку GlamDB и дашборд Live overview.

Повторный `up` сохраняет данные и пароль. Остановка:

```sh
sudo python3 scripts/observability.py down --config /etc/glamdb-observability.json
```

Она сохраняет постоянные тома, журналы Loki и секреты. Другой сервер требует
собственного конфига и диска; перенос данных — отдельная операция.
