package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// data source: https://github.com/danielfrg/espn-nba-scrapy/blob/master/data/teams.csv
// data source 2: https://www.kaggle.com/nogueira31/nba-players-rosters-20192020

//Team struct
type Team struct {
	Name         string
	City         string
	Abbreviation string
	PlayerList   []Player
}

//Player struct
type Player struct {
	Name      string
	Number    int
	Position  string
	Height    string
	Weight    int
	Birthdate string
}

func init() {
	// loads values from .env into the system
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
}

func csvParse() []Team {

	// Open the file
	teamcsv, err := os.Open("../../data/nba-teams.csv")
	if err != nil {
		log.Fatalln("Couldn't open the teams csv file", err)
	}

	// Parse the file
	r := csv.NewReader(teamcsv)
	//r := csv.NewReader(bufio.NewReader(csvfile))
	teamList := []Team{}
	// Iterate through the records
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		cityTeam := strings.Split(record[0], " ")
		if len(cityTeam) == 3 {
			cityTeam = []string{cityTeam[0] + " " + cityTeam[1], cityTeam[2]}
		}

		teamList = append(teamList, Team{cityTeam[1], cityTeam[0], strings.ToUpper(record[1]), []Player{}})
	}

	// File 2
	rostcsv, err := os.Open("../../data/nba-rosters.csv")
	if err != nil {
		log.Fatalln("Couldn't open the rosters csv file", err)
	}

	// Parse the file
	r2 := csv.NewReader(rostcsv)
	// Iterate through the records
	for {
		// Read each record from csv
		record, err := r2.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		playerNum, _ := strconv.Atoi(record[2])
		playerWeight, _ := strconv.Atoi(record[4])
		playerHeight := record[3][:len(record[3])-1]

		player := Player{record[1], playerNum, record[0], playerHeight, playerWeight, record[5]}

		playerCityTeam := strings.Split(record[6], " ")

		for i, val := range teamList {
			nameIdx := 1
			if len(playerCityTeam) == 3 {
				nameIdx = 2
			}
			if val.Name == playerCityTeam[nameIdx] {
				teamList[i].PlayerList = append(teamList[i].PlayerList, player)
			}
		}
	}
	return teamList
}

func main() {

	//connection to database
	dbConnect, _ := os.LookupEnv("DBCONNECTIONSTRING")

	client, err := mongo.NewClient(options.Client().ApplyURI(dbConnect))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cncl := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	_ = cncl

	defer client.Disconnect(ctx)
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}
	collection := client.Database("NBA").Collection("Team")

	////////

	// Listing available databases
	// databases, err := client.ListDatabaseNames(ctx, bson.M{})
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// Inserting one team
	// creating example player and team
	// cp3 := Player{Fname: "Chris", Lname: "Paul", Number: 3, PositionList: []string{"PG"}}
	// thunder := Team{
	// 	Name:         "Thunder",
	// 	City:         "Oklahoma City",
	// 	Abbreviation: "OKC",
	// 	PlayerList:   []Player{cp3},
	// }

	// insRes, err := collection.InsertOne(context.TODO(), thunder)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println("Inserted document: ", insRes.InsertedID)

	// insert many teams
	teamList := csvParse()

	var teams []interface{}
	for _, t := range teamList {
		teams = append(teams, t)
	}

	insResMult, err := collection.InsertMany(context.TODO(), teams)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Inserted documents: ", insResMult.InsertedIDs)

}
