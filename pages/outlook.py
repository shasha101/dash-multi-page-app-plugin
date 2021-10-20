import dash

dash.register_page(
	__name__,
	title = 'Forward Outlook',
	description = 'This is the forward outlook',  # should accept callable too
	order = 2,
	path = '/forward-outlook'
)


def layout():
	return 'Forward outlook'