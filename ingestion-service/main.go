package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
	"os"
	
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors" 
	_ "github.com/lib/pq"      	                  
	"github.com/redis/go-redis/v9"
)

type LogEntry struct {
	Service string `json:"service"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

// Struct for reading FROM Postgres
type DBLog struct {
	ID        int       `json:"id"`
	LogID     string    `json:"log_id"`
	Service   string    `json:"service"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	IsAnomaly bool      `json:"is_anomaly"`
	CreatedAt time.Time `json:"created_at"`
}

var ctx = context.Background()
var db *sql.DB

func initDB() {
	var err error
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "user=user password=password dbname=logs_db sslmode=disable host=localhost port=5432"
	}

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	
	if err = db.Ping(); err != nil {
		log.Fatal("Cannot connect to Postgres:", err)
	}
	fmt.Println("✅ Connected to Postgres")
}

func main() {
	redisAddr := os.Getenv("REDIS_URL")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	initDB()
	opt, _ := redis.ParseURL(redisAddr)
	rdb := redis.NewClient(opt)

	app := fiber.New()
	
	app.Use(cors.New())

	app.Post("/ingest", func(c *fiber.Ctx) error {
		var entry LogEntry
		if err := c.BodyParser(&entry); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
		}

		// Generate ID here to return to user immediately
		// In a real app, we might let Redis generate it, but this is fine.
		id, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "log_stream",
			Values: map[string]interface{}{
				"service": entry.Service,
				"level":   entry.Level,
				"message": entry.Message,
				"timestamp": time.Now().Unix(),
			},
		}).Result()

		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Redis unavailable"})
		}

		return c.JSON(fiber.Map{"status": "queued", "id": id})
	})

	// --- READ PATH (Dashboard) ---
	app.Get("/logs", func(c *fiber.Ctx) error {
		// Fetch last 50 logs, newest first
		rows, err := db.Query("SELECT id, log_id, service, level, message, is_anomaly, created_at FROM logs ORDER BY created_at DESC LIMIT 50")
		if err != nil {
			log.Println("Query Error:", err)
			return c.Status(500).JSON(fiber.Map{"error": "DB Query failed"})
		}
		defer rows.Close()

		var logs []DBLog
		for rows.Next() {
			var l DBLog
			if err := rows.Scan(&l.ID, &l.LogID, &l.Service, &l.Level, &l.Message, &l.IsAnomaly, &l.CreatedAt); err != nil {
				continue
			}
			logs = append(logs, l)
		}

		return c.JSON(logs)
	})

	log.Fatal(app.Listen(":3080"))
}