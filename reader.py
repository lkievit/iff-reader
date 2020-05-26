from datetime import date, timedelta
import re


class IFFReaderException(Exception):
    pass


class IFFReaderRowTypeException(IFFReaderException):
    pass


class IFFReader:
    REGEXP = r"@(?P<company_number_int>\d{3}),(?P<first_day_date>\d{8}),(?P<last_day_date>\d{8}),(?P<version_number_int>\d{4}),(?P<description_str>.{30})"

    @staticmethod
    def date(value):
        return date(int(value[4:8]), int(value[2:4]), int(value[:2]))

    @staticmethod
    def time(value):
        # We cannot use standard Python time objects here because some companies
        # have the switch from one 'travel day' to the next not at midnight and will
        # use time indications like "2510" to means "1:10 AM" on the night following the
        # calendar date. So, instead we use a timedelta relative to the start (00:00 AM) of the
        # calendar date to capture such times. This will probably have some interesting consequences
        # on nights when Summer/Daylight Savings time starts or ends.

        return timedelta(hours=int(value[:2]), minutes=int(value[2:4]))

    def parse_row(self, row, row_regexp):
        match = re.match(row_regexp, row)
        if not match:
            raise IFFReaderRowTypeException(f"Row {row} does not match {row_regexp}")

        matched_groups = match.groupdict()
        row_data = {}
        for group_name in matched_groups:
            name_parts = group_name.split("_")
            data_type = name_parts[-1]
            parameter_name = "_".join(name_parts[:-1])
            string_value = matched_groups[group_name].strip()
            if data_type == "str" or data_type == "strw":
                row_data[parameter_name] = string_value
            elif data_type == "int" or data_type == "intw":
                if not string_value:
                    row_data[parameter_name] = 0
                elif data_type == "intw" and string_value == "*":
                    row_data[parameter_name] = "*"
                else:
                    row_data[parameter_name] = int(string_value)
            elif data_type == "date":
                row_data[parameter_name] = self.date(string_value)
            elif data_type == "time":
                row_data[parameter_name] = self.time(string_value)
            else:
                raise ValueError(
                    "Unsupported data type in record type definition", data_type
                )

        return row_data

    def __init__(self, file, identification_row_missing=False):
        self._file = file
        self.pushed_back = []
        if not identification_row_missing:
            try:
                identification_row = self._file.__next__()
            except StopIteration:
                raise IFFReaderException("Missing identification record in file")
            self.delivery = self.parse_row(identification_row, IFFReader.REGEXP)
            self.start_date = self.delivery["first_day"]
            self.end_date = self.delivery["last_day"]

    def __iter__(self):
        return self

    def peek(self):
        line = self._file.__next__()
        self.pushed_back.append(line)
        return line

    def next_line(self):
        if self.pushed_back:
            line = self.pushed_back.pop()
            return line
        else:
            return self._file.__next__()

    def __next__(self):
        row = self.next_line()
        return self.parse_row(row, self.REGEXP)


class IFFCompanyReader(IFFReader):
    REGEXP = r"(?P<company_number_int>\d{3}),(?P<company_code_str>.{10}),(?P<company_name_str>.{30}),(?P<time_time>\d{4})"


class IFFChangesReader(IFFReader):
    REGEXP = r"#(?P<station_short_name_str>.{7})"
    CHANGE_REGEXP = r"\-(?P<from_service_identification_int>\d{8}),(?P<to_service_identification_int>\d{8}),(?P<possibility_to_change_trains_int>\d{1,2})"

    def __next__(self):
        row = self.next_line()
        change = self.parse_row(row, self.REGEXP)
        change["changes"] = []

        next_line = self.peek()
        while next_line[0] == "-":
            try:
                row = self.next_line()
                change["changes"] .append(self.parse_row(row, self.CHANGE_REGEXP))
                next_line = self.peek()
            except StopIteration:
                return change

        return change


class IFFCountryReader(IFFReader):
    REGEXP = r"(?P<country_code_str>.{4}),(?P<inland_int>\d),(?P<country_name_str>.{30})"


class IFFDeliveryReader(IFFReader):
    def __init__(self, file, *args, **kwargs):
        self.can_return = True
        super(IFFDeliveryReader, self).__init__(file, *args, **kwargs)

    def __next__(self):
        # Because we already read the identification record in the base reader, we have to fake
        # iteration a bit: we can return the identification in the first iteration loop
        # and after that all iteration stops. We use can_return to keep track of this.
        if self.can_return:
            self.can_return = False
            return self.delivery
        else:
            raise StopIteration()


class IFFFootnoteReader(IFFReader):
    REGEXP = r"#(?P<footnote_number_int>\d{5})"

    def date_range_generator(self):
        class DateIterator:
            def __init__(dself):
                dself.loop_date = self.start_date + timedelta(-1)

            def __iter__(dself):
                return dself

            def __next__(dself):
                if dself.loop_date == self.end_date:
                    raise StopIteration
                else:
                    dself.loop_date = dself.loop_date + timedelta(1)
                    return dself.loop_date

        return DateIterator()

    def __init__(self, file, *args, **kwargs):
        super(IFFFootnoteReader, self).__init__(file, *args, **kwargs)
        self.date_range = [d for d in self.date_range_generator()]

    def __next__(self):
        row = self.next_line()
        footnote = self.parse_row(row, self.REGEXP)

        vector = self.next_line().strip()

        date_vector = [dv for dv in zip(self.date_range, vector)]
        if len(vector.replace("1", "")) > len(vector.replace("0", "")):
            footnote["type"] = "only"
            included = [d for (d, v) in date_vector if v == "1"]
            footnote["included"] = included
        else:
            footnote["type"] = "except"
            excluded = [d for (d,v) in date_vector if v=="0"]
            footnote["excluded"] = excluded
        return footnote


class IFFLanguageReader(IFFReader):
    REGEXP = r"(?P<language_code_str>.{4}),(?P<description_str>.{30})"


class IFFStationReader(IFFReader):
    REGEXP = r"(?P<train_changes_int>\d),(?P<code_str>.{7}),(?P<transfer_time_int>\d{2}),(?P<max_time_int>\d{2}),(?P<country_code_str>.{4}),(?P<time_zone_int>\d{4}),(?P<attr_int>.{2}),(?P<x_int>[-\d]\d{5}),(?P<y_int>[-\d]\d{5}),(?P<name_str>.{30})"


class IFFStationConnectionReader(IFFReader):
    REGEXP = r">(?P<from_station_short_name_str>.{1,7}),(?P<to_station_short_name_str>.{1,7})"
    INFLECT_REGEXP = r"\&(?P<x_int>[-\d]\d{5}),(?P<y_int>[-\d]\d{5})"

    def __next__(self):
        row = self.next_line()
        connection = self.parse_row(row, self.REGEXP)
        connection["inflections"] = []

        next_line = self.peek()
        while next_line[0] == "&":
            try:
                row = self.next_line()
                connection["inflections"] .append(self.parse_row(row, self.INFLECT_REGEXP))
                next_line = self.peek()
            except StopIteration:
                return connection

        return connection


class IFFSynonymReader(IFFReader):
    TRANSPORT_ATTRIBUTE_REGEXP = r"\*(?P<transport_attribute_code_str>.{4}),(?P<language_code_str>.{4}),(?P<description_str>.{30})"
    TRANSPORT_MODE_REGEXP = r"\&(?P<transport_mode_code_str>.{4}),(?P<language_code_str>.{4}),(?P<description_str>.{30})"
    TRANSPORT_ATTRIBUTE_QUESTION_REGEXP = r"\$(?P<transport_attribute_q_code_str>.{4}),(?P<language_code_str>.{4}),(?P<description_str>.{30})"
    TRANSPORT_MODE_QUESTION_REGEXP = r"#(?P<transport_mode_question_code_str>.{4}),(?P<language_code_str>.{4}),(?P<description_str>.{30})"
    CONNECTION_MODE_REGEXP = r"\%(?P<connection_mode_code_str>.{4}),(?P<language_code_str>.{4}),(?P<description_str>.{30})"
    COUNTRY_REGEXP = r"\.(?P<counttry_code_str>.{4}),(?P<language_code_str>.{4}),(?P<description_str>.{30})"

    STATION_REGEXP = r"\+(?P<station_short_name_str>.{7}),(?P<language_code_str>.{4}),(?P<description_str>.{30})"
    GROUP_REGEXP = r"\-(?P<group_short_name_str>.{7}),(?P<language_code_str>.{4}),(?P<description_str>.{30})"

    def __next__(self):
        row = self.next_line()
        synonym_type = None
        if row[0] == "*":
            synonym_type = "transport_attribute"
            synonym = self.parse_row(row, self.TRANSPORT_ATTRIBUTE_REGEXP)
        elif row[0] == "&":
            synonym_type = "transport_mode"
            synonym = self.parse_row(row, self.TRANSPORT_MODE_REGEXP)
        elif row[0] == "$":
            synonym_type = "transport_attribute_question"
            synonym = self.parse_row(row, self.TRANSPORT_ATTRIBUTE_QUESTION_REGEXP)
        elif row[0] == "#":
            synonym_type = "transport_mode_question"
            synonym = self.parse_row(row, self.TRANSPORT_MODE_QUESTION_REGEXP)
        elif row[0] == "%":
            synonym_type = "connection_mode"
            synonym = self.parse_row(row, self.CONNECTION_MODE_REGEXP)
        elif row[0] == "+":
            synonym_type = "station"
            synonym = self.parse_row(row, self.STATION_REGEXP)
        elif row[0] == "-":
            synonym_type = "group"
            synonym = self.parse_row(row, self.GROUP_REGEXP)
        elif row[0] == ".":
            synonym_type = "country"
            synonym = self.parse_row(row, self.COUNTRY_REGEXP)
        synonym["synonym_type"] = synonym_type
        return synonym



class IFFTimeTablesReader(IFFReader):
    REGEXP = r"#(?P<service_identification_int>\d{8})"
    ATTRIBUTE_REGEXP = r"\*(?P<attribute_code_str>.{4}),(?P<first_stop_int>\d{3}),(?P<last_stop_int>\d{3})"
    NUMBER_REGEXP = r"\%(?P<company_number_int>\d{3}),(?P<service_number_int>\d{5}),(?P<variant_str>.{6,7}),(?P<first_stop_int>\d{3}),(?P<last_stop_int>\d{3}),(?P<service_name_str>.{30})"
    TRANSPORT_MODE_REGEXP = r"&(?P<transport_mode_code_str>.{4}),(?P<first_stop_int>\d{3}),(?P<last_stop_int>\d{3})"
    VALIDITY_REGEXP = r"\-(?P<footnote_number_int>\d{5}),(?P<first_stop_int>\d{3}),(?P<last_stop_int>\d{3})"
    START_REGEXP = r">(?P<station_short_name_str>.{7}),(?P<departure_time_time>\d{4})"
    FINAL_REGEXP = r"<(?P<station_short_name_str>.{7}),(?P<arrival_time_time>\d{4})"
    CONTINUATION_REGEXP = r"\.(?P<station_short_name_str>.{7}),(?P<time_time>\d{4})"
    PASSING_REGEXP = r";(?P<station_short_name_str>.{7})"
    INTERVAL_REGEXP = r"\+(?P<station_short_name_str>.{7}),(?P<arrival_time_time>\d{4}),(?P<departure_time_time>\d{4})"
    PLATFORM_REGEXP = r"\?(?P<arr_platform_name_str>.{5}),(?P<dep_platform_name_str>.{5}),(?P<footnote_number_int>\d{5})"

    def __next__(self):
        row = self.next_line()
        service = self.parse_row(row, self.REGEXP)

        service["numbers"] = []
        next_line = self.peek()
        while next_line[0] == "%":
            row = self.next_line()
            service["numbers"].append(self.parse_row(row, self.NUMBER_REGEXP))
            next_line = self.peek()

        service["validities"] = []
        while next_line[0] == "-":
            row = self.next_line()
            service["validities"].append(self.parse_row(row, self.VALIDITY_REGEXP))
            next_line = self.peek()

        service["transport_modes"] = []
        while next_line[0] == "&":
            row = self.next_line()
            service["transport_modes"].append(
                self.parse_row(row, self.TRANSPORT_MODE_REGEXP)
            )
            next_line = self.peek()

        service["attributes"] = []
        while next_line[0] == "*":
            row = self.next_line()
            service["attributes"].append(self.parse_row(row, self.ATTRIBUTE_REGEXP))
            next_line = self.peek()

        # route part should start now
        service["route"] = []
        stop_index = 0
        last_stop_done = False
        while not last_stop_done:
            try:
                row = self.next_line()
                stop = {}
                if next_line[0] == ">":
                    stop = self.parse_row(row, self.START_REGEXP)
                elif next_line[0] == "+":
                    stop = self.parse_row(row, self.INTERVAL_REGEXP)
                elif next_line[0] == ".":
                    stop = self.parse_row(row, self.CONTINUATION_REGEXP)
                elif next_line[0] == "<":
                    stop = self.parse_row(row, self.FINAL_REGEXP)
                    last_stop_done = True
                stop_index += 1
                stop["platform"] = []
                stop["stop_index"] = stop_index
                next_line = self.peek()
                if next_line[0] == "?":
                    row = self.next_line()
                    stop["platform"].append(self.parse_row(row, self.PLATFORM_REGEXP))
                    next_line = self.peek()

                service["route"].append(stop)

                while next_line[0] == ";":
                    row = self.next_line()
                    stop = self.parse_row(row, self.PASSING_REGEXP)
                    next_line = self.peek()
                    service["route"].append(stop)
            except StopIteration:
                # file end: ensure last created stop is included in the service
                if not stop in service["route"]:
                    service["route"].append(stop)
        return service


class IFFTimeZoneReader(IFFReader):
    REGEXP = r"#(?P<time_zone_number_int>\d{4})"
    PERIOD_REGEXP = r"(?P<time_difference_int>[\-\+]\d{2}),(?P<first_day_date>\d{8}),(?P<last_day_date>\d{8})"

    def __next__(self):
        row = self.next_line()
        timezone = self.parse_row(row, self.REGEXP)
        timezone["periods"] = []

        next_line = self.peek()
        while next_line[0] == "-" or next_line[0] == "+":
            try:
                row = self.next_line()
                timezone["periods"] .append(self.parse_row(row, self.PERIOD_REGEXP))
                next_line = self.peek()
            except StopIteration:
                return timezone

        return timezone

class IFFTransportAttributeReader(IFFReader):
    REGEXP = r"(?P<attribute_code_str>.{4}),(?P<processing_code_int>\d.{3}),(?P<description_str>.{30})"


class IFFTransportModeReader(IFFReader):
    REGEXP = r"(?P<transport_mode_code_str>.{4}),(?P<description_str>.{30})"


class IFFTransportModeQuestionReader(IFFReader):
    REGEXP = r"#(?P<question_code_str>.{4}),(?P<question_str>.{30})"
    TRANSPORT_MODE_REGEXP = r"\-(?P<transport_mode_code_str>.{4})"

    def __next__(self):
        row = self.next_line()
        question = self.parse_row(row, self.REGEXP)
        question["modes"] = []

        next_line = self.peek()
        while next_line[0] == "-":
            try:
                row = self.next_line()
                question["modes"] .append(self.parse_row(row, self.TRANSPORT_MODE_REGEXP))
                next_line = self.peek()
            except StopIteration:
                return question

        return question


class IFFTransportAttributeQuestionReader(IFFReader):
    REGEXP = r"#(?P<question_code_str>.{4}),(?P<question_type_int>[01]),(?P<question_str>.{30})"
    TRANSPORT_ATTRIBUTE_REGEXP = r"\-(?P<transport_attr_code_str>.{4})"

    def __next__(self):
        row = self.next_line()
        question = self.parse_row(row, self.REGEXP)
        question["attributes"] = []

        next_line = self.peek()
        while next_line[0] == "-":
            try:
                row = self.next_line()
                question["attributes"] .append(self.parse_row(row, self.TRANSPORT_ATTRIBUTE_REGEXP))
                next_line = self.peek()
            except StopIteration:
                return question

        return question


class IFFXChangesReader(IFFReader):
    REGEXP = r"#(?P<station_short_name_str>.{7})"
    XCHANGE_REGEXP = r"\-(?P<from_company_number_intw>\*\s\s|\d{3}),(?P<from_transport_mode_strw>.{4}),(?P<to_company_number_intw>\*\s\s|\d{3}),(?P<to_transport_mode_strw>.{4}),(?P<time_to_change_transport_int>\d{3}),(?P<footnote_number_int>\d{5})"

    def __next__(self):
        row = self.next_line()
        xchange = self.parse_row(row, self.REGEXP)
        xchange["xchanges"] = []

        next_line = self.peek()
        while next_line[0] == "-":
            try:
                row = self.next_line()
                xchange["xchanges"] .append(self.parse_row(row, self.XCHANGE_REGEXP))
                next_line = self.peek()
            except StopIteration:
                return xchange

        return xchange


class IFFXFootnoteReader(IFFFootnoteReader):
    pass


if __name__ == "__main__":
    with open(
        "import_files/ns/ndov/trnsaqst.dat", newline="", encoding="iso-8859-1"
    ) as iff_file:
        rdr = IFFTransportAttributeQuestionReader(iff_file)
        #import ipdb; ipdb.set_trace()
        for item in rdr:
            print(item)
            #exit(0)
