# ----------------------------------------------------------------------
# Etapa 1: Builder (Compila el binario estáticamente)
# ----------------------------------------------------------------------
    FROM golang:1.25.3-alpine AS builder

    # Establece el directorio de trabajo dentro del contenedor
    WORKDIR /app
    
    # Copia los archivos de módulos y descárgalos primero (para aprovechar el caché de Docker)
    COPY go.mod go.sum ./
    RUN go mod download
    
    # Copia todo el código fuente del proyecto
    COPY . .
    
    # Compila la aplicación.
    # CGO_ENABLED=0: Asegura que el binario sea estático, sin dependencias de librerías del sistema.
    # -a: Fuerza la reconstrucción
    # -ldflags: Reduce el tamaño final del binario al eliminar información de debug/símbolos.
    # -o: Nombre del binario de salida
    RUN CGO_ENABLED=0 go build -a -installsuffix cgo -ldflags '-s -w' -o /usr/local/bin/log-collector ./main.go
    
    # ----------------------------------------------------------------------
    # Etapa 2: Final (Binario pequeño y seguro)
    # ----------------------------------------------------------------------
    # Usamos un contenedor base minimalista (scratch o alpine). Alpine es más práctico
    # si necesitamos herramientas de diagnóstico mínimas, o 'scratch' para el mínimo absoluto.
    FROM alpine:latest
    
    # Crea un usuario no-root para seguridad
    RUN adduser -D collector
    USER collector
    
    # Copia solo el binario compilado de la etapa 'builder'
    COPY --from=builder /usr/local/bin/log-collector /usr/local/bin/log-collector
    
    # El ejecutable que se inicia cuando se lanza el contenedor
    ENTRYPOINT ["/usr/local/bin/log-collector"]
    
    # Expone el puerto (es solo documentación, no abre el puerto)
    EXPOSE 8080