<p align="center">

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
![Code Coverage](coverage.svg)

</p>

# Создание`.env`

Для корректной работы проекта необходимо создать файл `.env` в корне проекта со следующими переменными окружения:

```asciidoc
TOKENS_LIST=
PUBLIC_OR_LOCAL=
SERVICE_NAME=
HOST=
PORT=
VERIFY_TOKEN=
```

| Переменная        | Обязательность | Значение по умолчанию                | Описание                                                                  |
| ----------------- | -------------- |--------------------------------------| ------------------------------------------------------------------------- |
| `TOKENS_LIST`     | Обязательно    | —                                    | Ссылка на CSV со списком токенов. CSV должен содержать `token` и `source` |
| `PUBLIC_OR_LOCAL` | Необязательно  | LOCAL                                | Режим работы (например, `public` или `local`)                             |
| `SERVICE_NAME`    | Необязательно  | Имя сервиса по умолчанию из template | Имя сервиса для работы с токенами                                         |
| `HOST`            | Необязательно  | `localhost`                          | Хост для сервиса                                                          |
| `PORT`            | Необязательно  | `7070`                               | Порт для сервиса                                                          |
| `VERIFY_TOKEN`    | Необязательно  | `True`                               | Флаг проверки токена                                                      |


# Запуск на своей машине

#### Установка зависимостей
```bash
pip install pdm
pdm install
```


Активация окружения
```bash
source .venv/bin/activate
```


Запуск на своей машине
```bash
python -m src.server
```

После запуска ui микросервис адоступен по адресу
```bash
http://0.0.0.0:7070/template_fast_api/v1/#/
```


# Запуск контейнера публично

### Создаем .env в корне проекта на сервере
```asciidoc
TOKENS_LIST=
SERVICE_NAME=
```

### Строим контейнер
```bash
sudo docker build -t fast_api_template .
```
Узнаем его IMAGE ID 
```bash
sudo docker images
```

```bash
sudo docker run -d --env-file .env -p 7072:7070 <image_id>
```



# Запуск контейнера локально

### Строим контейнер
```bash
docker build -t fast_api_template .
```
Узнаем его ID
```bash
docker images
```

```bash
docker run -p 7071:7071 <IMAGE ID>
```