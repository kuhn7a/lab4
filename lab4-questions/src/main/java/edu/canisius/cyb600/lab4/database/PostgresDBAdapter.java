package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Category;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.Connection;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Postgres Implementation of the db adapter.
 */
public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }

    @Override
    public List<Film> getFilmsInCategory(Category category) {
        //Create a string with the sql statement
        String sql = "Select *\n" +
                "from category, film_category, film\n" +
                "where category.category_id  = film_category.category_id\n" +
                "and film.film_id = film_category.film_id\n" +
                "and category.category_id = ?\n";
        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            //Substitute a string for last name for the ? in the sql
            statement.setInt(1, category.getCategoryId());
            ResultSet results = statement.executeQuery();
            //Initialize an empty List to hold the return set of films.
            List<Film> films = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }
            //Return all the films.
            return films;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Actor> getActorsFirstNameStartingWithX(char firstLetter) {
        //Create a string with the sql statement
        String sql = "Select * from actor\n" +
                "where first_name like ?\n";
        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            //Substitute a string for last name for the ? in the sql
            statement.setString(1, firstLetter + "%");
            ResultSet results = statement.executeQuery();
            //Initialize an empty List to hold the return set of films.
            List<Actor> actors = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Actor actor = new Actor();
                actor.setActorId(results.getInt("ACTOR_ID"));
                actor.setFirstName(results.getString("FIRST_NAME"));
                actor.setLastName(results.getString("LAST_NAME"));
                actor.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                actors.add(actor);
            }
            //Return all the actors.
            return actors;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Film> getAllFilmsWithALengthLongerThanX(int length) {
        //Create a string with the sql statement
        String sql = "Select * from film\n" +
                "where length > ?\n";
        //Prepare the SQL statement with the code
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            //Substitute a string for last name for the ? in the sql
            statement.setInt(1, length);
            ResultSet results = statement.executeQuery();
            //Initialize an empty List to hold the return set of films.
            List<Film> films = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }
            //Return all the films.
            return films;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getAllDistinctCategoryNames() {
        //Prepare the SQL statement with the code
        try (Statement statement = conn.createStatement()) {
            //Initialize an empty List to hold the return set of category names.
            List<String> categoryNames = new ArrayList<>();
            ResultSet results = statement.executeQuery("select distinct name from category");
            //Loop through all the results and add the distinct name to the list
            while (results.next()) {
                //Add the category name to the categoryNames list
                categoryNames.add(results.getString("NAME"));
            }
            //Return all the distinct category names.
            return categoryNames;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Actor> insertAllActorsWithAnOddNumberLastName(List<Actor> actors) {
        String sql = "insert into actor (first_name, last_name) values (? , ? ) returning ACTOR_ID, LAST_UPDATE";
        //Initialize an empty List to hold all inserted actors
        List<Actor> insertedActors = new ArrayList<>();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            for (Actor actor : actors) {
                // Check if the last name length is odd
                if (actor.getLastName().length() % 2 != 0) {
                    statement.setString(1, actor.getFirstName());
                    statement.setString(2, actor.getLastName());
                    ResultSet resultSet = statement.executeQuery();

                    if (resultSet.next()) {
                        Actor insertedActor = new Actor(
                                resultSet.getInt("ACTOR_ID"),
                                actor.getFirstName(), // Currently being processed
                                actor.getLastName(), // Currently being processed
                                resultSet.getDate("LAST_UPDATE")
                        );
                        insertedActors.add(insertedActor);
                    }
                }
            }
        } catch(SQLException exception) {
            exception.printStackTrace();
        }
        return insertedActors;
    }
}
