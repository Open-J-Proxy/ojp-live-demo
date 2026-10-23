import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Small JSON codec for the dependency-free load producer. */
final class LoadJson {
    static Object parse(String text) {
        Parser parser = new Parser(text);
        Object value = parser.value();
        parser.space();
        if (parser.index != text.length()) throw parser.invalid();
        return value;
    }

    static String stringify(Object value) {
        StringBuilder out = new StringBuilder();
        write(out, value);
        return out.toString();
    }

    private static void write(StringBuilder out, Object value) {
        switch (value) {
            case null -> out.append("null");
            case String string -> quote(out, string);
            case Boolean bool -> out.append(bool);
            case Number number -> {
                if (number instanceof Double d && !Double.isFinite(d)
                        || number instanceof Float f && !Float.isFinite(f)) {
                    throw new IllegalArgumentException("Non-finite JSON number");
                }
                out.append(number);
            }
            case Map<?, ?> map -> {
                out.append('{');
                boolean first = true;
                for (var entry : map.entrySet()) {
                    if (!first) out.append(',');
                    first = false;
                    quote(out, entry.getKey().toString());
                    out.append(':');
                    write(out, entry.getValue());
                }
                out.append('}');
            }
            case Iterable<?> values -> {
                out.append('[');
                boolean first = true;
                for (Object item : values) {
                    if (!first) out.append(',');
                    first = false;
                    write(out, item);
                }
                out.append(']');
            }
            default -> throw new IllegalArgumentException("Unsupported JSON value: " + value.getClass());
        }
    }

    private static void quote(StringBuilder out, String value) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 32 || Character.isSurrogate(c)) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }

    private static final class Parser {
        final String text;
        int index;
        int depth;

        Parser(String text) { this.text = text; }
        IllegalArgumentException invalid() {
            return new IllegalArgumentException("Invalid JSON at offset " + index);
        }
        void space() {
            while (index < text.length() && " \t\r\n".indexOf(text.charAt(index)) >= 0) index++;
        }
        boolean take(char c) {
            if (index < text.length() && text.charAt(index) == c) { index++; return true; }
            return false;
        }
        void expect(char c) { if (!take(c)) throw invalid(); }
        Object value() {
            space();
            if (index >= text.length()) throw invalid();
            if (++depth > 256) throw invalid();
            try {
                return switch (text.charAt(index)) {
                    case '{' -> object();
                    case '[' -> array();
                    case '"' -> string();
                    case 't' -> literal("true", Boolean.TRUE);
                    case 'f' -> literal("false", Boolean.FALSE);
                    case 'n' -> literal("null", null);
                    default -> number();
                };
            } finally { depth--; }
        }
        Object literal(String token, Object result) {
            if (!text.startsWith(token, index)) throw invalid();
            index += token.length();
            return result;
        }
        Map<String, Object> object() {
            expect('{');
            Map<String, Object> result = new LinkedHashMap<>();
            space();
            if (take('}')) return result;
            do {
                space();
                String key = string();
                space();
                expect(':');
                result.put(key, value());
                space();
                if (take('}')) return result;
                expect(',');
            } while (true);
        }
        List<Object> array() {
            expect('[');
            List<Object> result = new ArrayList<>();
            space();
            if (take(']')) return result;
            do {
                result.add(value());
                space();
                if (take(']')) return result;
                expect(',');
            } while (true);
        }
        String string() {
            expect('"');
            StringBuilder out = new StringBuilder();
            while (index < text.length()) {
                char c = text.charAt(index++);
                if (c == '"') return out.toString();
                if (c < 32) throw invalid();
                if (c != '\\') { out.append(c); continue; }
                if (index >= text.length()) throw invalid();
                switch (text.charAt(index++)) {
                    case '"' -> out.append('"');
                    case '\\' -> out.append('\\');
                    case '/' -> out.append('/');
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'u' -> {
                        if (index + 4 > text.length()) throw invalid();
                        for (int i = index; i < index + 4; i++) {
                            char hex = text.charAt(i);
                            if (!(hex >= '0' && hex <= '9' || hex >= 'a' && hex <= 'f'
                                    || hex >= 'A' && hex <= 'F')) throw invalid();
                        }
                        try { out.append((char) Integer.parseInt(text.substring(index, index + 4), 16)); }
                        catch (NumberFormatException e) { throw invalid(); }
                        index += 4;
                    }
                    default -> throw invalid();
                }
            }
            throw invalid();
        }
        Number number() {
            int start = index;
            take('-');
            if (!take('0')) {
                if (index >= text.length() || text.charAt(index) < '1' || text.charAt(index) > '9')
                    throw invalid();
                digits();
            }
            boolean decimal = false;
            if (take('.')) { decimal = true; requiredDigits(); }
            if (take('e') || take('E')) {
                decimal = true;
                if (!take('+')) take('-');
                requiredDigits();
            }
            String token = text.substring(start, index);
            try {
                if (!decimal) {
                    try { return Long.parseLong(token); }
                    catch (NumberFormatException e) { return new java.math.BigInteger(token); }
                }
                return new BigDecimal(token);
            } catch (NumberFormatException e) { throw invalid(); }
        }
        void digits() {
            while (index < text.length() && text.charAt(index) >= '0' && text.charAt(index) <= '9') index++;
        }
        void requiredDigits() {
            int start = index;
            digits();
            if (start == index) throw invalid();
        }
    }
}
