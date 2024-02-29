//MongoDB ver 6.0

//Query 1
db.movies.aggregate([{
    "$match": {
        "actors": "Natalie Portman"
    }
}, {
    "$unwind": "$actors"
},
    {
        "$match": {
            "actors": {
                "$ne": "Natalie Portman"
            }
        }
    },
    {
        "$group": {
            "_id": "$actors",
            "movies": {
                "$addToSet": {
                    "title": "$title",
                    "year": "$year"
                }
            },
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$match": {
            "count": {
                "$gt": 2
            }
        }
    }
])

//Query 2
db.movies.aggregate([{
    "$unwind": "$genres"
}, {
    "$unwind": "$actors"
}, {
    "$group": {
        "_id": {
            "actor": "$actors",
            "genre": "$genres"
        },
        "count": {
            "$sum": 1
        }
    }
},
    {
        "$group": {
            "_id": "$_id.actor",
            "genres": {
                "$push": {
                    "genre": "$_id.genre",
                    "count": "$count"
                }
            },
            "total": {
                "$sum": "$count"
            }
        }
    },
    {
        "$sort": {
            "total": -1
        }
    }, {
        "$limit": 4
    }
])

//Query 3
db.movies.aggregate([{
    "$unwind": "$actors"
}, {
    "$group": {
        "_id": "$actors",
        "firstYear": {
            "$min": "$year"
        },
        "lastYear": {
            "$max": "$year"
        }
    }
},
    {
        "$project": {
            "_id": 1,
            "numYears": {
                "$subtract": ["$lastYear", "$firstYear"]
            },
            "firstYear": 1,
            "lastYear": 1
        }
    }, {
        "$sort": {
            "numYears": -1
        }
    },
    {
        "$limit": 4
    }
])

//Query 4
db.movies.aggregate([
    { "$match":
            {
                "metacritic":
                    {
                        "$exists": true
                    }
            }
    },
    {
        "$group": {
            "_id": "$director",
            "total_movies": { "$sum": 1 },
            "average_metacritic_score": { "$avg": "$metacritic" }
        }
    },
    { "$match":
            {
                "total_movies":
                    {
                        "$gt": 3
                    }
            }
    },
    { "$sort":
            { "average_metacritic_score": -1 }
    },
    { "$limit": 4 },
    {
        "$project": {
            "_id": 0,
            "director": "$_id",
            "total_movies": 1,
            "average_metacritic_score": 1
        }
    }
])

//Query 5
db.movies.aggregate([
    { "$match": { "genres": "comedy" } },
    {
        "$addFields": { "popularity": "$ratings.audience" }
    },
    {
        "$addFields": { "Wins_Nominations": { "$add": ["$awards.wins", "$awards.nominations"] } }
    },
    { "$sort": { "popularity": -1 } },
    { "$limit": 5 },
    {
        "$project": {
            "_id": 0,
            "Title": 1,
            "year": 1,
            "popularity": 1,
            "Wins_Nominations": 1
        }
    }
])

