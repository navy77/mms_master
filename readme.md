version 2.0.12\
docker compose build --no-cache\
docker compose up -d

for export image\
docker save --output mic_mms_app_210.tar mic/mms_app:2.0.12

for import image\
docker load -i mic_mms_app_212.tar