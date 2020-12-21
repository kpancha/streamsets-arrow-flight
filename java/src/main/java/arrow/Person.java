package arrow;

import java.util.Random;

public class Person {
    private static final String[] FIRST_NAMES = new String[]{"John", "Jane", "Gerard", "Aubrey", "Amelia"};
    private static final String[] LAST_NAMES = new String[]{"Smith", "Parker", "Phillips", "Jones"};

    private final String firstName;
    private final String lastName;
    private final int age;
    private final Address address;
    private static final Random rand = new Random();

    static Person randomPerson() {
        return new Person(
                FIRST_NAMES[rand.nextInt(FIRST_NAMES.length)],
                LAST_NAMES[rand.nextInt(LAST_NAMES.length)],
                rand.nextInt(120),
                Address.randomAddress()
        );
    }

    public Person(String firstName, String lastName, int age, Address address) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.address = address;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public int getAge() {
        return age;
    }


    public Address getAddress() {
        return address;
    }
}
