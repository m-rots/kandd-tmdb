package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sync"

	"github.com/panjf2000/ants/v2"
)

const headers = `@prefix tmm: <https://www.themoviedb.org/movie/> .
@prefix tmdb: <https://developers.themoviedb.org/3#> .
@prefix imdb: <https://www.imdb.com/interfaces/> .

tmdb:Movie rdf:type owl:Class .

tmdb:id rdf:type owl:DatatypeProperty .
tmdb:poster rdf:type owl:DatatypeProperty .
tmdb:lang rdf:type owl:DatatypeProperty .

imdb:id rdf:type owl:DatatypeProperty .

`

func main() {
	token := os.Args[1]
	if token == "" {
		fmt.Println("Please provide the TMDb API token as the first argument")
		os.Exit(1)
	}

	r, err := regexp.Compile(`imt:(tt\d+)`)
	if err != nil {
		panic(err)
	}

	file, err := os.Open("imdb.ttl")
	if err != nil {
		fmt.Println("Cannot open the imdb.ttl file")
		panic(err)
	}

	newFile, err := os.Create("tmdb.ttl")
	if err != nil {
		panic(err)
	}

	newFile.WriteString(headers)

	defer file.Close()

	var ids []string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		match := r.FindStringSubmatch(scanner.Text())
		if len(match) == 2 {
			ids = append(ids, match[1])
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup

	worker := func(imdbID interface{}) {
		defer wg.Done()

		imdb := imdbID.(string)

		tmdb, err := getTMDbID(imdb, token)
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		fmt.Printf("%s -> %d\n", imdb, tmdb.ID)

		newFile.WriteString(fmt.Sprintf(
			"tmm:%d rdf:type tmdb:Movie ; tmdb:id \"%d\" ; imdb:id \"%s\" ; tmdb:poster \"%s\" ; tmdb:lang \"%s\" .\n",
			tmdb.ID, tmdb.ID, imdb, tmdb.Poster, tmdb.Language,
		))
	}

	p, err := ants.NewPoolWithFunc(2, worker)

	defer p.Release()

	for _, imdb := range ids {
		wg.Add(1)
		p.Invoke(imdb)
	}

	wg.Wait()
}

type Movie struct {
	ID       int
	Poster   string `json:"poster_path"`
	Language string `json:"original_language"`
}

func getTMDbID(imdb string, token string) (*Movie, error) {
	url := fmt.Sprintf("https://api.themoviedb.org/3/find/%s?external_source=imdb_id", imdb)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("Weird status code %d for imdb: %s", res.StatusCode, imdb)
	}

	var response struct {
		MovieResults []Movie `json:"movie_results"`
	}

	json.NewDecoder(res.Body).Decode(&response)

	if len(response.MovieResults) == 0 {
		return nil, fmt.Errorf("No results for imdb: %s", imdb)
	}

	return &response.MovieResults[0], nil
}
