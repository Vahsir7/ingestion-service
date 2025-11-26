package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/gofiber/fiber/v2"

)

type LogEntry struct {
	Service   string    `json:"service"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
}

var ctx =  context.Background()

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Cannot connect to Redis", err)
	}
	app := fiber.New()

	app.Post("/ingest", func(c *fiber.Ctx) error {
		var entry LogEntry
		if err := c.BodyParser(&entry); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "Cannot parse JSON"})
		}

		id, err := rdb.XAdd(ctx, &redis.XAddArgs{
        Stream: "log_stream",
        Values: map[string]interface{}{
            "service": entry.Service,
            "level":   entry.Level,
            "message": entry.Message,
            "timestamp": time.Now().Unix(),
        },
    }).Result() // Change .Err() to .Result() to get the ID

    if err != nil {
        fmt.Println("Redis Error:", err) // Print error to terminal
        return c.Status(500).JSON(fiber.Map{"error": "Redis unavailable"})
    }

    fmt.Println("Data Written! Redis ID:", id) // Print ID to terminal
    return c.JSON(fiber.Map{"status": "queued", "id": id})
	})

	log.Fatal(app.Listen(":3080"))
}