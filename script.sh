#!/bin/bash


REPO_DIR="/# Путь к локальному репозиторию"
REPO_DIR2="/# Путь к локальному репозиторию2"

cd "$REPO_DIR" || exit

git fetch origin

LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse @{u})

if [ "$LOCAL" != "$REMOTE" ]; then
    echo "Обнаружены изменения в репозитории /GraidAis_Back. Выполняем git pull..."
    git pull
else
    echo "Изменений в репозитории /GraidAis_Back не обнаружено."
fi

systemctl restart flaskback.service || exit

cd "$REPO_DIR2" || exit

git fetch origin

LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse @{u})

if [ "$LOCAL" != "$REMOTE" ]; then
    echo "Обнаружены изменения в репозитории /GraidAIS_Frotend. Выполняем git pull..."
    git add .
    git commit -m"Какие-то изменения"
    git pull
else
    echo "Изменений в репозитории /GraidAIS_Frotend не обнаружено."
fi

npm install || exit
npm run build || exit
pm2 restart audiense_rating_next || exit