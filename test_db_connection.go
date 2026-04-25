//go:build ignore
// +build ignore

package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	fmt.Println("=== 测试数据库连接 ===\n")

	// 测试MySQL连接
	fmt.Println("1. 测试MySQL连接...")
	mysqlDSN := requiredEnv("LYNX_SQLSDK_MYSQL_DSN")
	mysqlDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		fmt.Printf("❌ MySQL连接失败: %v\n", err)
		os.Exit(1)
	}
	defer mysqlDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := mysqlDB.PingContext(ctx); err != nil {
		fmt.Printf("❌ MySQL Ping失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ MySQL连接成功")

	// 测试查询
	var result int
	if err := mysqlDB.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		fmt.Printf("❌ MySQL查询失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✅ MySQL查询成功，结果: %d\n", result)

	// 测试PostgreSQL连接
	fmt.Println("\n2. 测试PostgreSQL连接...")
	pgDSN := requiredEnv("LYNX_SQLSDK_POSTGRES_DSN")
	pgDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		fmt.Printf("❌ PostgreSQL连接失败: %v\n", err)
		os.Exit(1)
	}
	defer pgDB.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	if err := pgDB.PingContext(ctx2); err != nil {
		fmt.Printf("❌ PostgreSQL Ping失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ PostgreSQL连接成功")

	// 测试查询
	var result2 int
	if err := pgDB.QueryRowContext(ctx2, "SELECT 1").Scan(&result2); err != nil {
		fmt.Printf("❌ PostgreSQL查询失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✅ PostgreSQL查询成功，结果: %d\n", result2)

	fmt.Println("\n=== 所有数据库连接测试通过 ===")
}

func requiredEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		fmt.Printf("❌ 环境变量 %s 未设置\n", key)
		os.Exit(1)
	}
	return value
}
