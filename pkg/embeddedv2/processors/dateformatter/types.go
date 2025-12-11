package dateformatter

// TimeFormat represents the supported date/time format types.
type TimeFormat string

const (
	FormatLayout      TimeFormat = "Layout"
	FormatANSIC       TimeFormat = "ANSIC"
	FormatUnixDate    TimeFormat = "UnixDate"
	FormatRubyDate    TimeFormat = "RubyDate"
	FormatRFC822      TimeFormat = "RFC822"
	FormatRFC822Z     TimeFormat = "RFC822Z"
	FormatRFC850      TimeFormat = "RFC850"
	FormatRFC1123     TimeFormat = "RFC1123"
	FormatRFC1123Z    TimeFormat = "RFC1123Z"
	FormatRFC3339     TimeFormat = "RFC3339"
	FormatRFC3339Nano TimeFormat = "RFC3339Nano"
	FormatKitchen     TimeFormat = "Kitchen"
	FormatStamp       TimeFormat = "Stamp"
	FormatStampMilli  TimeFormat = "StampMilli"
	FormatStampMicro  TimeFormat = "StampMicro"
	FormatStampNano   TimeFormat = "StampNano"
	FormatDateTime    TimeFormat = "DateTime"
	FormatDateOnly    TimeFormat = "DateOnly"
	FormatTimeOnly    TimeFormat = "TimeOnly"
)

// DateStyle represents different ways to format dates.
type DateStyle string

const (
	DateStyleYYYY_MM_DD       DateStyle = "YYYY_MM_DD"
	DateStyleDD_MM_YYYY       DateStyle = "DD_MM_YYYY"
	DateStyleMM_DD_YYYY       DateStyle = "MM_DD_YYYY"
	DateStyleYYYY_MM_DD_Slash DateStyle = "YYYY_MM_DD_SLASH"
	DateStyleDD_MM_YYYY_Slash DateStyle = "DD_MM_YYYY_SLASH"
	DateStyleMM_DD_YYYY_Slash DateStyle = "MM_DD_YYYY_SLASH"
)

// TimeStyle represents different ways to format times.
type TimeStyle string

const (
	TimeStyle24Hour   TimeStyle = "24_HOUR"
	TimeStyle12Hour   TimeStyle = "12_HOUR"
	TimeStyle24HourHM TimeStyle = "24_HOUR_HM"
	TimeStyle12HourHM TimeStyle = "12_HOUR_HM"
)

// GetTimeFormatLayout returns the Go time layout string for a given format.
func GetTimeFormatLayout(format TimeFormat) string {
	layouts := map[TimeFormat]string{
		FormatLayout:      "01/02 03:04:05PM '06 -0700",
		FormatANSIC:       "Mon Jan _2 15:04:05 2006",
		FormatUnixDate:    "Mon Jan _2 15:04:05 MST 2006",
		FormatRubyDate:    "Mon Jan 02 15:04:05 -0700 2006",
		FormatRFC822:      "02 Jan 06 15:04 MST",
		FormatRFC822Z:     "02 Jan 06 15:04 -0700",
		FormatRFC850:      "Monday, 02-Jan-06 15:04:05 MST",
		FormatRFC1123:     "Mon, 02 Jan 2006 15:04:05 MST",
		FormatRFC1123Z:    "Mon, 02 Jan 2006 15:04:05 -0700",
		FormatRFC3339:     "2006-01-02T15:04:05Z07:00",
		FormatRFC3339Nano: "2006-01-02T15:04:05.999999999Z07:00",
		FormatKitchen:     "3:04PM",
		FormatStamp:       "Jan _2 15:04:05",
		FormatStampMilli:  "Jan _2 15:04:05.000",
		FormatStampMicro:  "Jan _2 15:04:05.000000",
		FormatStampNano:   "Jan _2 15:04:05.000000000",
		FormatDateTime:    "2006-01-02 15:04:05",
		FormatDateOnly:    "2006-01-02",
		FormatTimeOnly:    "15:04:05",
	}
	if layout, ok := layouts[format]; ok {
		return layout
	}
	return ""
}

// GetDateStyleLayout returns the Go time layout for a given date style.
func GetDateStyleLayout(style DateStyle) string {
	layouts := map[DateStyle]string{
		DateStyleYYYY_MM_DD:       "2006-01-02",
		DateStyleDD_MM_YYYY:       "02-01-2006",
		DateStyleMM_DD_YYYY:       "01-02-2006",
		DateStyleYYYY_MM_DD_Slash: "2006/01/02",
		DateStyleDD_MM_YYYY_Slash: "02/01/2006",
		DateStyleMM_DD_YYYY_Slash: "01/02/2006",
	}
	if layout, ok := layouts[style]; ok {
		return layout
	}
	return ""
}

// GetTimeStyleLayout returns the Go time layout for a given time style.
func GetTimeStyleLayout(style TimeStyle) string {
	layouts := map[TimeStyle]string{
		TimeStyle24Hour:   "15:04:05",
		TimeStyle12Hour:   "03:04:05 PM",
		TimeStyle24HourHM: "15:04",
		TimeStyle12HourHM: "03:04 PM",
	}
	if layout, ok := layouts[style]; ok {
		return layout
	}
	return ""
}

// IsValidTimeFormat checks if a format string is valid.
func IsValidTimeFormat(format string) bool { return GetTimeFormatLayout(TimeFormat(format)) != "" }

// IsValidDateStyle checks if a date style string is valid.
func IsValidDateStyle(style string) bool {
	if style == "" {
		return true
	}
	return GetDateStyleLayout(DateStyle(style)) != ""
}

// IsValidTimeStyle checks if a time style string is valid.
func IsValidTimeStyle(style string) bool {
	if style == "" {
		return true
	}
	return GetTimeStyleLayout(TimeStyle(style)) != ""
}
