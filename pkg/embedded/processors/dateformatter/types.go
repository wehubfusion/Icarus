package dateformatter

// TimeFormat represents the supported date/time format types
type TimeFormat string

// Predefined time format constants matching Go's time package
const (
	// Standard Go time layouts
	FormatLayout      TimeFormat = "Layout"      // "01/02 03:04:05PM '06 -0700" Reference time
	FormatANSIC       TimeFormat = "ANSIC"       // "Mon Jan _2 15:04:05 2006"
	FormatUnixDate    TimeFormat = "UnixDate"    // "Mon Jan _2 15:04:05 MST 2006"
	FormatRubyDate    TimeFormat = "RubyDate"    // "Mon Jan 02 15:04:05 -0700 2006"
	FormatRFC822      TimeFormat = "RFC822"      // "02 Jan 06 15:04 MST"
	FormatRFC822Z     TimeFormat = "RFC822Z"     // "02 Jan 06 15:04 -0700"
	FormatRFC850      TimeFormat = "RFC850"      // "Monday, 02-Jan-06 15:04:05 MST"
	FormatRFC1123     TimeFormat = "RFC1123"     // "Mon, 02 Jan 2006 15:04:05 MST"
	FormatRFC1123Z    TimeFormat = "RFC1123Z"    // "Mon, 02 Jan 2006 15:04:05 -0700"
	FormatRFC3339     TimeFormat = "RFC3339"     // "2006-01-02T15:04:05Z07:00"
	FormatRFC3339Nano TimeFormat = "RFC3339Nano" // "2006-01-02T15:04:05.999999999Z07:00"
	FormatKitchen     TimeFormat = "Kitchen"     // "3:04PM"

	// Stamp formats
	FormatStamp      TimeFormat = "Stamp"      // "Jan _2 15:04:05"
	FormatStampMilli TimeFormat = "StampMilli" // "Jan _2 15:04:05.000"
	FormatStampMicro TimeFormat = "StampMicro" // "Jan _2 15:04:05.000000"
	FormatStampNano  TimeFormat = "StampNano"  // "Jan _2 15:04:05.000000000"

	// Custom formats with style support
	FormatDateTime TimeFormat = "DateTime" // "2006-01-02 15:04:05" (can be customized with date_style and time_style)
	FormatDateOnly TimeFormat = "DateOnly" // "2006-01-02" (can be customized with date_style)
	FormatTimeOnly TimeFormat = "TimeOnly" // "15:04:05" (can be customized with time_style)
)

// DateStyle represents different ways to format dates
type DateStyle string

const (
	// Date styles with dashes
	DateStyleYYYY_MM_DD DateStyle = "YYYY_MM_DD" // "2006-01-02"
	DateStyleDD_MM_YYYY DateStyle = "DD_MM_YYYY" // "02-01-2006"
	DateStyleMM_DD_YYYY DateStyle = "MM_DD_YYYY" // "01-02-2006"

	// Date styles with slashes
	DateStyleYYYY_MM_DD_Slash DateStyle = "YYYY_MM_DD_SLASH" // "2006/01/02"
	DateStyleDD_MM_YYYY_Slash DateStyle = "DD_MM_YYYY_SLASH" // "02/01/2006"
	DateStyleMM_DD_YYYY_Slash DateStyle = "MM_DD_YYYY_SLASH" // "01/02/2006"
)

// TimeStyle represents different ways to format times
type TimeStyle string

const (
	TimeStyle24Hour   TimeStyle = "24_HOUR"    // "15:04:05"
	TimeStyle12Hour   TimeStyle = "12_HOUR"    // "03:04:05 PM"
	TimeStyle24HourHM TimeStyle = "24_HOUR_HM" // "15:04" (hours and minutes only)
	TimeStyle12HourHM TimeStyle = "12_HOUR_HM" // "03:04 PM" (hours and minutes only)
)

// GetTimeFormatLayout returns the Go time layout string for a given format
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
	return "" // Empty string indicates unknown format
}

// GetDateStyleLayout returns the Go time layout for a given date style
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
	return "" // Empty string indicates unknown style
}

// GetTimeStyleLayout returns the Go time layout for a given time style
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
	return "" // Empty string indicates unknown style
}

// IsValidTimeFormat checks if a format string is valid
func IsValidTimeFormat(format string) bool {
	return GetTimeFormatLayout(TimeFormat(format)) != ""
}

// IsValidDateStyle checks if a date style string is valid
func IsValidDateStyle(style string) bool {
	if style == "" {
		return true // Empty is valid (means use default)
	}
	return GetDateStyleLayout(DateStyle(style)) != ""
}

// IsValidTimeStyle checks if a time style string is valid
func IsValidTimeStyle(style string) bool {
	if style == "" {
		return true // Empty is valid (means use default)
	}
	return GetTimeStyleLayout(TimeStyle(style)) != ""
}
