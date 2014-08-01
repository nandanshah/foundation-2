package com.dla.foundation.elasticsearch.mlt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.dla.foundation.elasticsearch.entities.Cast;
import com.dla.foundation.elasticsearch.entities.Crew;
import com.dla.foundation.elasticsearch.entities.Genre;
import com.dla.foundation.elasticsearch.entities.Movie;

public class MoreLikeThisFieldQuery {

	private String _index;
	private String _type;
	private Client client;

	public MoreLikeThisFieldQuery(String _index, String _type, Client client) {
		this._index = _index;
		this._type = _type;
		this.client = client;
	}

	String getQueryFromGuid(int guid, int size) {
		String movieResult = this.getMovieDetails(guid).getSourceAsString();
		Movie movie = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			movie = objectMapper.readValue(movieResult, Movie.class);
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String title = movie.getTitle();
		List<Genre> genres = movie.getGenres();
		String genresString = "";
		for (Genre genre : genres) {
			genresString += genre.getName() + ", ";
		}
		List<Crew> directorsList = movie.getCredits().getCrewByDepartment(
				"Directing");
		String directors = "";
		for (Crew c : directorsList) {
			directors += c.getName();
		}
		String leadActors = "";
		List<Cast> leadActorsList = movie.getCredits().getCastByOrder(3);
		for (Cast c : leadActorsList) {
			leadActors += c.getName() + ", ";
		}
		QueryBuilder similarityQuery = this.getSimilarityQuery(title,
				genresString, directors, leadActors);

		return similarityQuery.toString();
	}

	public SearchResponse getMoreLikeThisMovies(int guid, int size) throws JsonParseException, JsonMappingException, IOException {
		String movieResult = this.getMovieDetails(guid).getSourceAsString();
		Movie movie = null;
		ObjectMapper objectMapper = new ObjectMapper();
		movie = objectMapper.readValue(movieResult, Movie.class);
		String title = movie.getTitle();
		List<Genre> genres = movie.getGenres();
		String genresString = "";
		for (Genre genre : genres) {
			genresString += genre.getName() + ", ";
		}
		List<Crew> directorsList = movie.getCredits().getCrewByDepartment(
				"Directing");
		String directors = "";
		for (Crew c : directorsList) {
			directors += c.getName();
		}
		String leadActors = "";
		List<Cast> leadActorsList = movie.getCredits().getCastByOrder(3);
		for (Cast c : leadActorsList) {
			leadActors += c.getName() + ", ";
		}
		QueryBuilder similarityQuery = this.getSimilarityQuery(title,
				genresString, directors, leadActors);

		// System.out.println(similarityQuery);

		SearchResponse mltSearchResponse = this.client
				.prepareSearch(this._index).setQuery(similarityQuery)
				.setSize(size).execute().actionGet();

		// System.out.println(mltSearchResponse.toString());
		for (SearchHit s : mltSearchResponse.getHits()) {
			String source = s.getSourceAsString();
			Movie result = null;
			try {
				result = objectMapper.readValue(source, Movie.class);
			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			StringBuilder resultString = new StringBuilder();
			resultString.append(result.getTitle() + "\t");
			for (Genre g : result.getGenres()) {
				resultString.append(g.getName() + ",");
			}
			resultString.replace(resultString.length() - 1,
					resultString.length(), "\t");
			for (Cast c : result.getCredits().getCastByOrder(4)) {
				resultString.append(c.getName() + ",");
			}
			resultString.replace(resultString.length() - 1,
					resultString.length(), "\t");
			for (Crew dir : result.getCredits()
					.getCrewByDepartment("Directing")) {
				resultString.append(dir.getName() + ",");
			}
			resultString.replace(resultString.length() - 1,
					resultString.length(), "");
			// System.out.println(resultString.toString());
		}
		return mltSearchResponse;

	}

	// Main query which wraps title,genre,actors,directors field queries
	// genres, directors, actors must be comma separated string of values
	QueryBuilder getSimilarityQuery(String title, String genres,
			String directors, String leadActors) {
		QueryBuilder titleQuery = this.getTitleFieldQuery(title);
		QueryBuilder genreQuery = this.getGenreFieldQuery(genres);
		QueryBuilder directorQuery = this.getDirectorFieldQuery(directors);
		QueryBuilder leadActorsQuery = this.getLeadActorFieldQuery(leadActors);
		return QueryBuilders.boolQuery().should(titleQuery).should(genreQuery)
				.should(directorQuery).should(leadActorsQuery)
				.minimumNumberShouldMatch(2);

	}

	// MLT field query on movie title
	QueryBuilder getTitleFieldQuery(String title) {
		QueryBuilder q = QueryBuilders.moreLikeThisFieldQuery("title")
				.likeText(title).minTermFreq(1).minDocFreq(1)
				.percentTermsToMatch(0.75f).boostTerms(2).boost(5);
		return q;
	}

	// MLT field query on movie genre
	QueryBuilder getGenreFieldQuery(String genres) {
		QueryBuilder q = QueryBuilders.moreLikeThisFieldQuery("genres.name")
				.likeText(genres).minTermFreq(1).minDocFreq(1)
				.percentTermsToMatch(0.5f).boostTerms(2).boost(2);
		return q;

	}

	// MLT field query on directors of the movie.
	QueryBuilder getDirectorFieldQuery(String director) {
		QueryBuilder directorFieldQuery = QueryBuilders
				.moreLikeThisFieldQuery("credits.crew.name").likeText(director)
				.minTermFreq(1).minDocFreq(1).percentTermsToMatch(0.0f);
		QueryBuilder matchQuery = QueryBuilders.matchQuery(
				"credits.crew.department", "Direction");
		QueryBuilder boolQuery = QueryBuilders.boolQuery()
				.must(directorFieldQuery).must(matchQuery);
		QueryBuilder nestedQuery = QueryBuilders.nestedQuery("credits.crew",
				boolQuery).scoreMode("avg");
		return nestedQuery;
	}

	// MLT field query on lead actors of the movie.
	QueryBuilder getLeadActorFieldQuery(String actorName) {
		QueryBuilder directorFieldQuery = QueryBuilders
				.moreLikeThisFieldQuery("credits.cast.name")
				.likeText(actorName).minTermFreq(1).minDocFreq(1)
				.percentTermsToMatch(0.0f);
		QueryBuilder rangeQuery = QueryBuilders
				.rangeQuery("credits.cast.order").lt(4);
		QueryBuilder boolQuery = QueryBuilders.boolQuery()
				.must(directorFieldQuery).must(rangeQuery);
		QueryBuilder nestedQuery = QueryBuilders.nestedQuery("credits.cast",
				boolQuery).scoreMode("avg");
		return nestedQuery;

	}

	// Fetches movie details from ES server
	// guid - movie ID of the movie who's details are to be retrieved
	GetResponse getMovieDetails(int guid) {

		GetResponse movieDetails = this.client
				.prepareGet(this._index, this._type, guid + "").execute()
				.actionGet();
		// System.out.println(movieDetails.toString());
		return movieDetails;
	}

	// helper method to write to CSV file
	static void writeToCSVFile(String content, String fileName) {
		try {

			File file = new File(fileName);

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(content);
			bw.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
