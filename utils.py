def convertir_xlsx_a_google(ruta_origen,id_destino):

    '''
    Convierte archivos xlsx a hojas de google.
    '''

    !pip install -U -q google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib > /DEV/NULL

    # Autenticación
    from google.colab import auth
    auth.authenticate_user()

    # Montar Google Drive
    from google.colab import drive
    drive.mount('/content/drive')

    # Importar librerías necesarias
    import os
    import google.auth
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    # Crear servicio de Google Drive
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
    drive_service = build("drive", "v3", credentials=creds)
    for archivo in os.listdir(ruta_origen):
        if archivo.endswith(".xlsx"):
            ruta_archivo = os.path.join(ruta_origen, archivo)
            nombre_base = os.path.splitext(archivo)[0]

            metadata = {
                "name": nombre_base,
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "parents": [id_destino]
            }

            media = MediaFileUpload(ruta_archivo, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", resumable=True)
            nuevo_archivo = drive_service.files().create(body=metadata, media_body=media, fields="id, name").execute()

            print(f"Convertido y subido: {nuevo_archivo['name']} (ID: {nuevo_archivo['id']})")