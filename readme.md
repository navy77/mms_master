version 2.0.15\
docker compose build --no-cache\
docker compose up -d

for export image\
docker save --output mic_mms_app_215.tar mic/mms_app:2.0.15

for import image\
docker load -i mic_mms_app_215.tar
