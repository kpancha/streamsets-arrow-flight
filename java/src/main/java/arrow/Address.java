package arrow;

import java.util.Random;

public class Address {

  private static final String[] STREETS = new String[]{
          "Halloway",
          "Sunset Boulvard",
          "Wall Street",
          "Secret Passageway"
  };
  private static final String[] CITIES = new String[]{
          "Brussels",
          "Paris",
          "London",
          "Amsterdam"
  };

  private final String street;
  private final int streetNumber;
  private final String city;
  private final int postalCode;
  private static final Random rand = new Random();

  static Address randomAddress() {
    String randomStreet = STREETS[rand.nextInt(STREETS.length)];
    String randomCity = CITIES[rand.nextInt(CITIES.length)];
    return new Address(
            randomStreet,
            rand.nextInt(3000) + 1,
            randomCity,
            rand.nextInt(9000) + 1000
    );
  }

  public Address(String street, int streetNumber, String city, int postalCode) {
    this.street = street;
    this.streetNumber = streetNumber;
    this.city = city;
    this.postalCode = postalCode;
  }

  public String getStreet() {
    return street;
  }

  public int getStreetNumber() {
    return streetNumber;
  }

  public String getCity() {
    return city;
  }

  public int getPostalCode() {
    return postalCode;
  }
}
