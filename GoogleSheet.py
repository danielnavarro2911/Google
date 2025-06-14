

from google.colab import auth
import gspread
from google.auth import default
import pandas as pd
from gspread_dataframe import set_with_dataframe , get_as_dataframe
from googleapiclient.discovery import build
import os
import io
from googleapiclient.http import MediaIoBaseDownload
import numpy as np
import string
from openpyxl import load_workbook
import openpyxl



class GoogleSheet:
    def __init__(self):
        auth.authenticate_user()
        creds, _ = default()
        self.gc = gspread.authorize(creds)
        self.drive_service = build('drive', 'v3', credentials=creds)
    
    def show_shared_files(self):
        results = self.drive_service.files().list(q="sharedWithMe = true and trashed = false",
                                     fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])
        if not items:
            print('No files found.')
        else:
            print('Files shared with me:')
            for item in items:
                print(f'{item["name"]} ({item["id"]})')
    
    def download_files(self,folder_id,download_path = 'descargas',specific_type = None):
        '''
        Descarga todos los documentos que hayan en el folder_id indicado, o si se especifica un tipo en specific_type, solo los de ese tipo.

        ejemplo: download_files('xyz','descargas_pdfs','pdf')

        esto descarga unicamente los pdfs de la carpeta xyz en el directorio /content/descargas_pdfs
        '''
        download_path = '/content/'+download_path

        os.makedirs(download_path, exist_ok=True)  # Crea la carpeta si no existe

        # Buscar todos los archivos dentro de la carpeta especificada
        query = f"'{folder_id}' in parents and mimeType = 'application/{specific_type}' and trashed = false" if specific_type else f"'{folder_id}' in parents and trashed = false"
        results = self.drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
        files_to_download = results.get('files', [])

        if not files_to_download:
            print(f'No se encontraron archivos en la carpeta con ID: {folder_id}')
        else:
            print(f'Se encontraron {len(files_to_download)} archivos en la carpeta. Iniciando descarga:')
            for file_info in files_to_download:
                file_id = file_info['id']
                file_name = file_info['name']
                mime_type = file_info['mimeType']
                file_path = os.path.join(download_path, file_name)
                #print(f'Descargando: {file_name} ({file_id}), tipo: {mime_type}')

                try:
                    request = self.drive_service.files().get_media(fileId=file_id)
                    fh = io.FileIO(file_path, 'wb')
                    downloader = MediaIoBaseDownload(fh, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
                        #print(f'Descarga de {file_name}: {int(status.progress() * 100)}%.', end='\r')
                    #print(f'Descarga de {file_name}: 100% completada.')
                except Exception as e:
                    print(f'Error al descargar {file_name}: {e}')

        print(f'\nTodos los archivos se han intentado descargar en: {download_path}')
    
    def connect_with_spreadsheet(self,url_or_name):
        self.url_or_name=url_or_name

        if self.url_or_name.startswith('https://'):
            self.spreadsheet = self.gc.open_by_url(self.url_or_name)
        else:
            self.spreadsheet = self.gc.open(self.url_or_name)
        print('Conexión lista')
    def get_data(self,sheet_name):
        self.sheet_name=sheet_name#.replace() \xa0
        self.ws=self.spreadsheet.worksheet(self.sheet_name)
        return get_as_dataframe(self.ws)
    def get_sheet_names(self):
        return list(map(lambda x: x.title,self.spreadsheet.worksheets()))
    def save_data(self,df,sheet_name,rows="5000", cols="20",start_cell=None):
        try:
            self.spreadsheet.add_worksheet(title=sheet_name, rows=rows,cols=cols)

        except:
            pass
        finally:
            new_ws=self.spreadsheet.worksheet(sheet_name)
            if start_cell:
                cell=new_ws .acell(start_cell)
                row=cell.row
                col=cell.col
                set_with_dataframe(new_ws,df,row=row,col=col,include_column_header=False)
            else:
                set_with_dataframe(new_ws,df)
    def get_data_from_excel(self,excel,sheet,drop_hidden_rows=True,drop_hidden_columns=True):
        

        workbook = openpyxl.load_workbook(excel)
        worksheet = workbook[sheet]

        # Identificar filas ocultas
        hidden_rows_idx = []
        if drop_hidden_rows:
            
            for row_idx, row_dim in worksheet.row_dimensions.items():
                if row_dim.hidden:
                    hidden_rows_idx.append(row_idx-1)  # openpyxl es 1-based, pandas es 0-based
        hidden_cols_idx = []
        if drop_hidden_columns:
            
            for col_letter, col_dim in worksheet.column_dimensions.items():
                if col_dim.hidden:
                    try:
                        col_idx = string.ascii_uppercase.index(col_letter.upper())
                        hidden_cols_idx.append(col_idx)
                    except:pass



        df = pd.read_excel(excel,sheet_name=sheet)


        
        df=df.iloc[:, ~np.isin(range(df.shape[1]), hidden_cols_idx)]

        for i in hidden_rows_idx:
            try:
                df=df.drop(i)
            except:
                pass

        df=df.reset_index(drop=True)

        return df
