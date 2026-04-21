Quick implementation of a route optimization algorithm using the US Census API. The script takes an Excel file with locations, optimizes the route, and outputs a CSV file with the optimized route and an HTML file with a Leaflet/OpenStreetMap visualization.

As described in route_optimizer.py, run the script with something like the following command:
`python route_optimizer.py  --input ../RuralMapping.xlsx --output route.csv --map-output route_map.html`