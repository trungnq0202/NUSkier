import java.util.Random;

public class RandomGenerator {
    private static final Random random = new Random();

    public static Integer generateSkierID() {
        return random.nextInt(100000) + 1;  // SkierID between 1 and 100000
    }

    public static Integer generateResortID() {
        return random.nextInt(10) + 1;  // ResortID between 1 and 10
    }

    public static Integer generateLiftID() {
        return random.nextInt(40) + 1;  // LiftID between 1 and 40
    }

    public static Integer generateSeasonID() {
        return 2024;  // SeasonID is fixed at 2024
    }

    public static Integer generateDayID() {
        return 1;  // DayID is fixed at 1
    }

    public static Integer generateTime() {
        return random.nextInt(360) + 1;  // Time between 1 and 360
    }
}
