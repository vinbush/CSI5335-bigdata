function prepData() {
	$("tr.thead").remove(); // delete the intermediate headers (they're for readability)
	$("td:contains(TOT)") // find elements that contain 'TOT' (we want the per-team rows for players, to get both park factors)
                .filter(function() { return $(this).children().length === 0;}) // just in case TOT appears in some other field? You can't be too careful
                .parent() // get the whole row
				.remove(); // 86 'em
	$("table").prepend("<thead></thead>"); // add a thead so DataTables works properly
	$("tr").first().appendTo("thead"); // move the first row (which contains the headers) to the thead
	$('td[data-append-csv][data-stat="player"]').text(function() { // replace player names with the player ID's
		return $(this).data("appendCsv");
	});
	$("tbody a").replaceWith(function() { // get rid of links
		return this.childNodes;
	});
	$('table').DataTable(); // set up DataTable
}