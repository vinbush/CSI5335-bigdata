function fetchYear () {
	$('#tableHolder').html('');

	var year = $("#yearInput").val().trim();
	var url = "http://widgets.sports-reference.com/wg.fcgi?site=br&url=%2Fleagues%2FMLB%2F" + year + "-batting-pitching.shtml&div=div_players_batting_pitching&del_col=1,3,5,6,7,8,19,20,21,22,23,30";

	// use postscribe lib to allow writing from async script
	postscribe('#tableHolder', '<script src="' + url + '"></script>');

	// store the year in a data attribute on the table holder
	$('#tableHolder').data('year', year);

	/*$("#tableHolder").html("");
	$("<script/>", {
		type: "text/javascript",
		src: url
	}).appendTo("#tableHolder");*/
}

function prepData() {
	$("tr.thead").remove(); // delete the intermediate headers (they're for readability)
	$("td:contains(TOT)") // find elements that contain 'TOT' (we want the per-team rows for players, to get both park factors)
                .filter(function() { return $(this).children().length === 0;}) // just in case TOT appears in some other field. You can't be too careful
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

	// change column names to match Lahman
	$('th').map(function() {
		var stat = $(this).data('stat');
		switch (stat) {
			case 'player':
				$(this).text('playerID');
				break;
			case 'team_ID':
				$(this).text('teamID');
				break;
			case 'GIDP':
				$(this).text('GIDP');
				break;
		}
	});


	// add a year column
	var year = $('#tableHolder').data('year');
	$("tbody tr").prepend("<td>" + year + "</td>");
	$("thead tr").prepend("<th>yearID</th>")
	
	// set up DataTable
	$('table').DataTable({
		dom: 'Bfrtip',
		buttons: ['csv']
	});
}