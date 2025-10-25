package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os" // <- IMPORTACIÓN AÑADIDA
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gorilla/mux"
)

// --- CONFIGURACIÓN Y CANALES ---
const (
	BatchSize  = 10000           // Insertar después de 10,000 logs
	FlushDelay = 5 * time.Second // Insertar cada 5 segundos
	TableName  = "kubernetes_ingress_logs"
	HTTPPort   = ":8080"
)

// LogChan es el canal de logs (buffer interno)
var LogChan = make(chan IngressLog, 100000)

func main() {
	// 1. Conexión a ClickHouse
	conn, err := connectClickHouse()
	if err != nil {
		fmt.Printf("❌ Error al conectar con ClickHouse: %v\n", err)
		return
	}
	defer conn.Close()

	// 2. Ejecutar el worker de ingesta en segundo plano (goroutine)
	go IngestWorker(conn)

	// 3. Configurar el servidor HTTP
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/ingest", IngestHandler).Methods("POST")

	fmt.Printf("🚀 Colector de logs iniciado en el puerto %s\n", HTTPPort)
	fmt.Printf("   El worker de batching se ejecuta en paralelo (tamaño: %d / tiempo: %s)\n", BatchSize, FlushDelay)

	// Iniciar el servidor
	if err := http.ListenAndServe(HTTPPort, router); err != nil {
		fmt.Printf("❌ Error al iniciar el servidor HTTP: %v\n", err)
	}
}

// --------------------------------------------------------------------------------
// --- HTTP HANDLER (RECEPCIÓN DE LOGS DE FLUENT BIT) ---
// --------------------------------------------------------------------------------

// IngestHandler recibe un POST con un array de logs (típico de Fluent Bit o HTTP batching).
func IngestHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var logs []IngressLog

	// Decodificar el JSON del cuerpo de la petición
	if err := json.NewDecoder(r.Body).Decode(&logs); err != nil {
		http.Error(w, "JSON invalido o formato incorrecto", http.StatusBadRequest)
		return
	}

	count := 0
	// Iterar sobre los logs recibidos y enviarlos al canal del worker
	for _, log := range logs {
		// Protección: si el canal está lleno, se omite el log para evitar bloquear el servidor HTTP.
		// En producción, se debería usar un sistema de reintentos o un buffer más grande.
		select {
		case LogChan <- log:
			count++
		default:
			// El canal está lleno, se dropea el log o se loggea una advertencia
			fmt.Println("⚠️ Advertencia: Canal de logs lleno. Droppeando log para no bloquear el HTTP.")
		}
	}

	// Responder rápidamente
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"status": "accepted", "logs_processed": %d}`, count)
}

// --------------------------------------------------------------------------------
// --- CLICKHOUSE CONNECTION Y WORKER (EL CÓDIGO DE BATCHING) ---
// --------------------------------------------------------------------------------

func connectClickHouse() (clickhouse.Conn, error) {
	// 🐛 CORREGIDO: Usar variables de entorno inyectadas por Kubernetes
	host := os.Getenv("CLICKHOUSE_HOST")
	port := os.Getenv("CLICKHOUSE_PORT")

	if host == "" || port == "" {
		// Fallback para desarrollo o error en la inyección de env
		return nil, fmt.Errorf("variables de entorno CLICKHOUSE_HOST o CLICKHOUSE_PORT no definidas")
	}

	addr := fmt.Sprintf("%s:%s", host, port)

	fmt.Printf("ℹ️ Intentando conectar a ClickHouse en: %s\n", addr) // Log de depuración

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}
	return conn, nil
}

// IngestWorker gestiona el buffer y la inserción masiva.
func IngestWorker(conn clickhouse.Conn) {
	var logBuffer []IngressLog
	ticker := time.NewTicker(FlushDelay)
	defer ticker.Stop()

	for {
		select {
		case log := <-LogChan:
			// 1. Recibir log y agregarlo al buffer
			logBuffer = append(logBuffer, log)

			// 2. Si el buffer alcanza el tamaño, realizar la inserción (Flush)
			if len(logBuffer) >= BatchSize {
				fmt.Printf("📦 Batch Size alcanzado (%d logs). Insertando...\n", len(logBuffer))
				flushBatch(conn, &logBuffer)
			}

		case <-ticker.C:
			// 3. Si se cumple el tiempo, insertar el lote restante (Flush por tiempo)
			if len(logBuffer) > 0 {
				fmt.Printf("⏳ Flush por tiempo. Insertando %d logs...\n", len(logBuffer))
				flushBatch(conn, &logBuffer)
			}
		}
	}
}

// flushBatch realiza la inserción masiva a ClickHouse.
func flushBatch(conn clickhouse.Conn, logBuffer *[]IngressLog) {
	ctx := context.Background()

	// 1. Preparar el batch
	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", TableName))
	if err != nil {
		fmt.Printf("❌ Error al preparar el batch: %v\n", err)
		return
	}

	// 2. Llenar el batch
	for _, log := range *logBuffer {
		// Usamos AppendStruct para aprovechar las etiquetas 'ch' en la estructura.
		if err := batch.AppendStruct(&log); err != nil {
			fmt.Printf("❌ Error al adjuntar log: %v\n", err)
			continue
		}
	}

	// 3. Enviar el batch
	if err := batch.Send(); err != nil {
		fmt.Printf("❌ Error al enviar el batch a ClickHouse: %v\n", err)
	} else {
		fmt.Printf("✅ %d logs insertados correctamente.\n", len(*logBuffer))
	}

	// 4. Resetear el buffer
	*logBuffer = (*logBuffer)[:0]
}
