package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var schema = `
CREATE TABLE bank_user (
		id SERIAL PRIMARY KEY,
    first_name text,
    last_name text,
    email text
);

CREATE TABLE account (
    id SERIAL PRIMARY KEY,
    number numeric,
    balance double precision DEFAULT 0.00,
    closed boolean DEFAULT FALSE,
    user_id integer REFERENCES bank_user(id)
);

CREATE TABLE transaction (
    id SERIAL PRIMARY KEY,
    sum double precision,
    create_at date DEFAULT NOW(),
    from_account_id int REFERENCES account(id),
    to_account_id int REFERENCES account(id)
);

INSERT INTO bank_user (email, first_name, last_name) VALUES ('admin@bank.com', 'Admin', 'Admin');

INSERT INTO account (number, user_id) VALUES (1100000, 1)`

// User in bank
type User struct {
	Id        int
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Email     string
}

// Account in bank
type Account struct {
	Id      int
	Number  uint32
	Balance float64
	Closed  bool
	UserID  int `db:"user_id"`
}

// Transaction between accounts
type Transaction struct {
	Id       int
	Sum      float64
	From     int    `db:"from_account_id" bson:",omitempty"`
	To       int    `db:"to_account_id"`
	CreateAt string `db:"create_at"`
}

func generateRandomBytes() ([]byte, error) {
	b := make([]byte, 4)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

func generateRandomInt() uint32 {
	b, _ := generateRandomBytes()
	return binary.BigEndian.Uint32(b)
}

func startServer(port string) {
	http.HandleFunc("/user", userHandler)
	http.HandleFunc("/accounts", accountsHandler)
	http.HandleFunc("/account", accountHandler)
	http.HandleFunc("/close_account", closeAccountHandler)
	http.HandleFunc("/balance", balanceHandler)
	http.HandleFunc("/refill", refillHandler)
	http.HandleFunc("/transactions", transactionsHandler)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatalln(err)
	}
}

func success(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
}

func fail(w http.ResponseWriter) {
	w.WriteHeader(http.StatusInternalServerError)
}

func transactionsHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method == http.MethodGet {
		datesStart, _ := r.URL.Query()["dateStart"]
		dateStart := datesStart[0]
		datesFinish, _ := r.URL.Query()["dateFinish"]
		dateFinish := datesFinish[0]

		getTransactions(dateStart, dateFinish, w)
	}

	if r.Method == http.MethodPost {
		IDsFrom, _ := r.URL.Query()["account_from"]
		accountIDFrom, _ := strconv.ParseUint(IDsFrom[0], 10, 32)
		IDsTo, _ := r.URL.Query()["account_to"]
		accountIDTo, _ := strconv.ParseUint(IDsTo[0], 10, 32)
		accountIDFromInt := uint32(accountIDFrom)
		accountIDToInt := uint32(accountIDTo)
		sums, _ := r.URL.Query()["sum"]
		sum, _ := strconv.ParseFloat(sums[0], 64)
		accountFrom, accountTo := getFromTo(accountIDFromInt, accountIDToInt)
		sendMoney(accountFrom, accountTo, sum, w)
	}
}

func getFromTo(from uint32, to uint32) (int, int) {
	var fromID int
	var toID int
	db := getDB()
	rows, _ := db.Query("SELECT id FROM account WHERE number = $1 AND closed = FALSE", from)
	for rows.Next() {
		rows.Scan(&fromID)
		//fmt.Println(err)
	}
	rows, _ = db.Query("SELECT id FROM account WHERE number = $1 AND closed = FALSE", to)
	for rows.Next() {
		rows.Scan(&toID)
		//fmt.Println(err)
	}

	return int(fromID), int(toID)
}

func sendMoney(from int, to int, sum float64, w http.ResponseWriter) {
	fmt.Println(from)
	balance := getBalance(from, w)
	if balance > sum {
		refillAccount(from, to, sum, w)
	} else {
		fail(w)
	}
}

func getTransactions(start string, finish string, w http.ResponseWriter) {
	db := getDB()
	transactions := []Transaction{}
	error := db.Select(&transactions, "SELECT * FROM transaction WHERE create_at BETWEEN $1 AND $2", start, finish)
	fmt.Println(error)
	b, err := json.Marshal(transactions)
	if err != nil {
		fmt.Println(err)
		return
	}
	success(w)
	fmt.Fprintln(w, string(b))
}

func refillHandler(w http.ResponseWriter, r *http.Request) {
	IDs, _ := r.URL.Query()["to_account_id"]
	ID, _ := strconv.Atoi(IDs[0])
	if r.Method == http.MethodPost {
		sums, _ := r.URL.Query()["sum"]
		sum, _ := strconv.ParseFloat(sums[0], 64)
		refillAccount(ID, 1, sum, w)
	}
}

func refillAccount(accountIDFrom int, accountIDTo int, sum float64, w http.ResponseWriter) {
	db := getDB()
	transaction := &Transaction{0, sum, accountIDTo, accountIDFrom, ""} //from_account_id?????
	tx := db.MustBegin()
	_, err := tx.NamedExec("INSERT INTO transaction (sum, to_account_id, from_account_id) VALUES (:sum, :to_account_id, :from_account_id)", transaction)
	tx.Commit()
	if accountIDTo != 1 {
		tx := db.MustBegin()
		balance := getBalance(accountIDFrom, w)
		tx.MustExec("UPDATE account SET balance = $1 WHERE id = $2", balance-sum, accountIDFrom)
		tx.Commit()
		success(w)
	}

	if err != nil {
		fmt.Println(err)
		fail(w)
	} else {
		success(w)
		updateBalance(accountIDFrom, sum, w)
		fmt.Fprintln(w, "Account refilled by cash:", sum)
	}
}

func balanceHandler(w http.ResponseWriter, r *http.Request) {
	IDs, _ := r.URL.Query()["account_id"]
	ID, _ := strconv.Atoi(IDs[0])
	if r.Method == http.MethodGet {
		balance := getBalance(ID, w)
		fmt.Fprintln(w, balance)
		return
	}

	// if r.Method == http.MethodPut {
	// 	sums, _ := r.URL.Query()["sum"]
	// 	sum, _ := strconv.ParseFloat(sums[0], 64)
	// 	updateBalance(ID, sum, w)
	// 	return
	// }
}

func updateBalance(accountID int, sum float64, w http.ResponseWriter) {
	db := getDB()
	tx := db.MustBegin()
	balance := getBalance(accountID, w)
	tx.MustExec("UPDATE account SET balance = $1 WHERE id = $2", balance+sum, accountID)
	tx.Commit()
	success(w)
	//fmt.Fprintln(w, "Account closed")
}

func getBalance(accountID int, w http.ResponseWriter) float64 {
	var balance float64
	db := getDB()
	rows, err := db.Query("SELECT balance FROM account WHERE id = $1 AND closed = FALSE", accountID)
	if err != nil {
		fail(w)
	} else {
		success(w)
	}
	//fmt.Println(err)
	for rows.Next() {
		rows.Scan(&balance)
		//fmt.Println(err)
	}
	//fmt.Fprintln(w, balance)
	return balance
}

func accountsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		userIDs, _ := r.URL.Query()["id"]
		userID, _ := strconv.Atoi(userIDs[0])
		getAccounts(userID, w)
		return
	}
}

func accountHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		IDs, _ := r.URL.Query()["id"]
		ID, _ := strconv.Atoi(IDs[0])
		getAccount(ID, w)
		return
	}

	if r.Method == http.MethodPost {
		userIDs, _ := r.URL.Query()["user_id"]
		userID, _ := strconv.Atoi(userIDs[0])
		createAccount(userID, w)
		return
	}

}

func closeAccountHandler(w http.ResponseWriter, r *http.Request) {
	IDs, _ := r.URL.Query()["id"]
	ID, _ := strconv.Atoi(IDs[0])
	closeAccount(ID, w)
}

func closeAccount(accountID int, w http.ResponseWriter) {
	db := getDB()
	tx := db.MustBegin()
	tx.MustExec("UPDATE account SET closed = $1 WHERE id = $2", true, accountID)
	tx.Commit()
	success(w)
	fmt.Fprintln(w, "Account closed")
}

func createAccount(userID int, w http.ResponseWriter) {
	db := getDB()
	number := generateRandomInt()
	account := &Account{0, number, 0.00, false, userID}
	tx := db.MustBegin()
	_, err := tx.NamedExec("INSERT INTO account (number, user_id) VALUES (:number, :user_id)", account)
	tx.Commit()
	if err != nil {
		fmt.Println(err)
		fail(w)
	} else {
		success(w)
		fmt.Fprintln(w, "Account created")
	}
}

func getAccount(ID int, w http.ResponseWriter) {
	db := getDB()
	accounts := []Account{}
	db.Select(&accounts, "SELECT * FROM account WHERE id = $1 AND WHERE closed = 0", ID)
	b, err := json.Marshal(accounts)
	if err != nil {
		fmt.Println(err)
		return
	}
	// add transactions to output
	success(w)
	fmt.Fprintln(w, string(b))
}

func getAccounts(ID int, w http.ResponseWriter) {
	db := getDB()
	accounts := []Account{}
	db.Select(&accounts, "SELECT * FROM account WHERE user_id = $1", ID)
	b, err := json.Marshal(accounts)
	if err != nil {
		fmt.Println(err)
		return
	}
	success(w)
	fmt.Fprintln(w, string(b))
}

func userHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		emails, ok := r.URL.Query()["email"]
		if !ok || len(emails[0]) < 1 {
			getUser("", w)
		} else {
			getUser(emails[0], w)
		}
		return
	}

	if r.Method == http.MethodPost {
		emails, ok := r.URL.Query()["email"]
		if !ok || len(emails[0]) < 1 { 
			return
		}
		firstNames, _ := r.URL.Query()["first_name"]
		lastNames, _ := r.URL.Query()["last_name"]
		email := emails[0]
		firstName := firstNames[0]
		lastName := lastNames[0]
		createUser(email, firstName, lastName, w)
		return
	}

	if r.Method == http.MethodPut {
		ids, ok := r.URL.Query()["id"]
		if !ok || len(ids[0]) < 1 { 
			return
		}
		emails, ok := r.URL.Query()["email"]
		if !ok || len(emails[0]) < 1 { 
			return
		}
		firstNames, _ := r.URL.Query()["first_name"]
		lastNames, _ := r.URL.Query()["last_name"]
		email := emails[0]
		firstName := firstNames[0]
		lastName := lastNames[0]
		id, _ := strconv.Atoi(ids[0])

		updateUser(id, email, firstName, lastName, w)
		return
	}

	if r.Method == http.MethodDelete {
		ids, ok := r.URL.Query()["id"]
		if !ok || len(ids[0]) < 1 { 
			return
		}
		id, _ := strconv.Atoi(ids[0])
		deleteUser(id, w)
	}

}

func getUser(email string, w http.ResponseWriter) {
	db := getDB()
	people := []User{}
	if email == "" {
		db.Select(&people, "SELECT * FROM bank_user")
	} else {
		db.Select(&people, "SELECT * FROM bank_user WHERE email = $1", email)
	}
	b, err := json.Marshal(people)
	if err != nil {
		fmt.Println(err)
		return
	}
	success(w)
	fmt.Fprintln(w, string(b))
}

func deleteUser(id int, w http.ResponseWriter) {
	db := getDB()
	tx := db.MustBegin()
	tx.MustExec("DELETE FROM bank_user WHERE id = $1", id)
	tx.Commit()
	success(w)
	fmt.Fprintln(w, "User deleted")
}

func updateUser(id int, email string, firstName string, lastName string, w http.ResponseWriter) {
	db := getDB()
	tx := db.MustBegin()
	tx.MustExec("UPDATE bank_user SET email = $1, first_name = $2, last_name = $3 WHERE id = $4", email, firstName, lastName, id)
	tx.Commit()
	success(w)
	fmt.Fprintln(w, "User updated")
}

func createUser(email string, firstName string, lastName string, w http.ResponseWriter) {
	db := getDB()
	user := &User{0, firstName, lastName, email}
	tx := db.MustBegin()
	_, err := tx.NamedExec("INSERT INTO bank_user (first_name, last_name, email) VALUES (:first_name, :last_name, :email)", user)
	tx.Commit()
	if err != nil {
		fail(w)
	} else {
		success(w)
	}
	fmt.Fprintln(w, "User added")
}

func getDB() *sqlx.DB {
	db, err := sqlx.Connect("postgres", "user=dante dbname=superbank sslmode=disable")
	if err != nil {
		log.Fatalln(err)
	}
	return db
}

func runMigration() {
	db := getDB()
	err := db.MustExec(schema)
	fmt.Println(err)
}

func main() {
	startServer("8080")
	//runMigration()
}
