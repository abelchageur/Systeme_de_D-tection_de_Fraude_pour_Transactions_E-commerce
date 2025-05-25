# from fastapi import FastAPI, UploadFile, File
# import shutil
# import os

# app = FastAPI()

# UPLOAD_DIR = "/app/data"
# os.makedirs(UPLOAD_DIR, exist_ok=True)

# @app.post("/upload/")
# async def upload_file(file: UploadFile = File(...)):
#     file_path = os.path.join(UPLOAD_DIR, file.filename)
#     with open(file_path, "wb") as buffer:
#         shutil.copyfileobj(file.file, buffer)
#     return {"message": f"File '{file.filename}' uploaded successfully!", "path": file_path}
##############################################################################""
from fastapi import FastAPI
import pandas as pd
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/csv-data/")
def get_csv_data():
    data = pd.read_csv("/app/data/Fraudulent_E-Commerce_Transaction_Data_2.csv")
    return JSONResponse(content=data.to_dict(orient="records"))
################################################################################
# from fastapi import FastAPI, HTTPException
# from fastapi.responses import StreamingResponse
# import pandas as pd
# import os

# app = FastAPI()

# # Function to check file size
# def is_file_large(file_path, size_threshold=50*1024*1024):
#     """
#     Check if the file is larger than the specified threshold.
#     Default threshold is 50 MB.
#     """
#     return os.path.getsize(file_path) > size_threshold

# # Stream CSV in chunks
# def stream_csv(input_file, chunk_size=50000):
#     # Open the file and stream it chunk by chunk
#     with pd.read_csv(input_file, chunksize=chunk_size) as reader:
#         for chunk in reader:
#             yield chunk.to_csv(index=False, header=False)

# @app.get("/csv-data/")
# def get_csv_data():
#     input_file = "/app/data/Fraudulent_E-Commerce_Transaction_Data.csv"
    
#     # Check if the file exists
#     if not os.path.exists(input_file):
#         raise HTTPException(status_code=404, detail="CSV file not found")

#     # Check if the file is large
#     if is_file_large(input_file):
#         # Stream the file without loading everything into memory
#         return StreamingResponse(stream_csv(input_file), media_type="text/csv")
    
#     # If the file is small, load and return data
#     data = pd.read_csv(input_file)
#     return {"data": data.to_dict(orient="records")}
