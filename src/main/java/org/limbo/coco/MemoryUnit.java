package org.limbo.coco;

public enum MemoryUnit {

    /**
     * b
     */
    BYTES(0, 'b') {

        public long toBytes(final long amount) { return amount; }

        public long toKiloBytes(final long amount) { return safeShift(amount, KILOBYTES.offset - BYTES.offset); }

        public long toMegaBytes(final long amount) { return safeShift(amount, MEGABYTES.offset - BYTES.offset); }

        public long toGigaBytes(final long amount) { return safeShift(amount, GIGABYTES.offset - BYTES.offset); }
    },

    /**
     * k (1024 BYTES)
     */
    KILOBYTES(BYTES.offset + MemoryUnit.OFFSET, 'k') {

        public long toBytes(final long amount) { return safeShift(amount, BYTES.offset - KILOBYTES.offset); }

        public long toKiloBytes(final long amount) { return amount; }

        public long toMegaBytes(final long amount) { return safeShift(amount, MEGABYTES.offset - KILOBYTES.offset); }

        public long toGigaBytes(final long amount) { return safeShift(amount, GIGABYTES.offset - KILOBYTES.offset); }
    },

    /**
     * M (1024 KILOBYTES)
     */
    MEGABYTES(KILOBYTES.offset + MemoryUnit.OFFSET, 'm') {

        public long toBytes(final long amount) { return safeShift(amount, BYTES.offset - MEGABYTES.offset); }

        public long toKiloBytes(final long amount) { return safeShift(amount, KILOBYTES.offset - MEGABYTES.offset); }

        public long toMegaBytes(final long amount) { return amount; }

        public long toGigaBytes(final long amount) { return safeShift(amount, GIGABYTES.offset - MEGABYTES.offset); }
    },

    /**
     * G (1024 MEGABYTES)
     */
    GIGABYTES(MEGABYTES.offset + MemoryUnit.OFFSET, 'g') {

        public long toBytes(final long amount) { return safeShift(amount, BYTES.offset - GIGABYTES.offset); }

        public long toKiloBytes(final long amount) { return safeShift(amount, KILOBYTES.offset - GIGABYTES.offset); }

        public long toMegaBytes(final long amount) { return safeShift(amount, MEGABYTES.offset - GIGABYTES.offset); }

        public long toGigaBytes(final long amount) { return amount; }
    };

    private static final int OFFSET = 10;

    private        final int offset;
    private        final char unit;

    private MemoryUnit(final int offset, final char unit) {
        this.offset = offset;
        this.unit = unit;
    }

    /**
     * Retrieves the unit character for the MemoryUnit
     * @return the unit character
     */
    public char getUnit() {
        return unit;
    }

    public abstract long toBytes(long amount);
    public abstract long toKiloBytes(long amount);
    public abstract long toMegaBytes(long amount);
    public abstract long toGigaBytes(long amount);

    public String toString(final long amount) {
        return amount + Character.toString(this.unit);
    }

    /**
     * Returns the MemoryUnit instance based on provided char
     * @param unit the unit to look for
     * @return the MemoryUnit instance matching the unit
     * @throws IllegalArgumentException if no matching MemoryUnit matching the char
     */
    public static MemoryUnit forUnit(final char unit) throws IllegalArgumentException {
        for (MemoryUnit memoryUnit : values()) {
            if (memoryUnit.unit == unit) {
                return memoryUnit;
            }
        }
        throw new IllegalArgumentException("'" + unit + "' suffix doesn't match any SizeUnit");
    }

    /**
     * Parses the unit part of a String, if no unit char available, returns {@link MemoryUnit#BYTES}
     * @param value the String representation of an amount of memory
     * @return the MemoryUnit instance, or BYTES if none
     */
    public static MemoryUnit parseUnit(final String value) {
        if (hasUnit(value)) {
            return forUnit(Character.toLowerCase(value.charAt(value.length() - 1)));
        }
        return BYTES;
    }

    /**
     * Parses the amount represented by the string, without caring for the unit
     * @param value the String representation of an amount of memory
     * @return the amount of mem in the unit represented by the potential unit char
     * @throws NumberFormatException if not a number (with potential unit char stripped)
     */
    public static long parseAmount(final String value) throws NumberFormatException {
        if (value == null) {
            throw new NullPointerException("Value can't be null!");
        }

        if (value.length() == 0) {
            throw new IllegalArgumentException("Value can't be an empty string!");
        }

        if (hasUnit(value)) {
            return Long.parseLong(value.substring(0, value.length() - 1).trim());
        } else {
            return Long.parseLong(value);
        }
    }

    /**
     * Parses the string for its content, returning the represented value in bytes
     * @param value the String representation of an amount of memory
     * @return the amount of bytes represented by the string
     * @throws NumberFormatException if not a number (with potential unit char stripped)
     * @throws IllegalArgumentException if no matching MemoryUnit matching the char
     */
    public static long parseSizeInBytes(final String value) throws NumberFormatException, IllegalArgumentException {
        if (value.length() == 0) {
            throw new IllegalArgumentException("Value can't be an empty string!");
        }

        MemoryUnit memoryUnit = parseUnit(value);
        return memoryUnit.toBytes(parseAmount(value));
    }

    private static boolean hasUnit(final String value) {
        if (value.length() > 0) {
            char potentialUnit = value.charAt(value.length() - 1);
            return potentialUnit < '0' || potentialUnit > '9';
        }
        return false;
    }

    private static long safeShift(final long unit, final long shift) {
        if (shift > 0) {
            return unit >>> shift;
        } else if (shift <= -1 * Long.numberOfLeadingZeros(unit)) {
          return Long.MAX_VALUE;
        } else {
            return unit << -shift;
        }
    }
}
