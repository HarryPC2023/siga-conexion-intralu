# Imagen oficial de Playwright: ya trae Python + Chromium + todas las
# dependencias del sistema que Playwright necesita (fuentes, librerías
# gráficas, etc.) — evita tener que instalarlas a mano una por una.
FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

WORKDIR /app

# Copiamos primero solo requirements.txt para aprovechar el cache de
# Docker: si el código cambia pero las dependencias no, Railway no
# vuelve a reinstalar todo desde cero en cada deploy.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# El navegador Chromium ya viene instalado en la imagen base de
# Playwright — no hace falta "playwright install" de nuevo.

COPY recoleccion_notas.py .

# Railway inyecta la variable PORT en tiempo de ejecución (no la
# conocemos todavía al construir la imagen), así que el comando de
# arranque debe leerla en shell, no como argumento fijo.
CMD ["sh", "-c", "python recoleccion_notas.py"]
