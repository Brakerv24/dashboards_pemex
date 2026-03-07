# Usamos una imagen ligera de Python
FROM python:3.12.4-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos los archivos de requerimientos primero para aprovechar la caché
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto de las carpetas y scripts (data, modules, app.py, etc.)
COPY . .

# Exponemos el puerto que usa FastAPI/Uvicorn
EXPOSE 8000

# Comando para arrancar la app
CMD ["python", "app.py"]