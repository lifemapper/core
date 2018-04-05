/*
Copyright (C) 2017, University of Kansas Center for Research

Lifemapper Project, lifemapper [at] ku [dot] edu,
Biodiversity Institute,
1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or (at
your option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
02110-1301, USA.
*/

"use strict";

var maps = {};
var mapLayers = {};

document.onmousemove = document.onmouseup = document.onmousedown = function(event) {
    const plot = document.getElementById("plot");
    if (plot == null) return;
    const rect = plot.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    app.ports.mouseEvent.send([event.type, x, y]);
};

function configureMap(element) {
    var map = maps[element._leaflet_id];
    if (map == null) return;
    console.log("updating leaflet id", element._leaflet_id);

    var layers = mapLayers[element._leaflet_id];
    if (layers != null) {
        layers.forEach(function(layer) {  map.removeLayer(layer); });
    }

    const node = nodeLookup.find(function(d) { return d.header == element.dataset["mapColumn"]; });
    const dataColumn = node && node.index;

    console.log("adding layer");

    // if (layers == null || layers.length === 0) {
        mapLayers[element._leaflet_id] = [
            L.geoJSON(ancPam, {
                style: style(dataColumn),
                filter: function(feature) {
                    const data = feature.properties.data;
                    for (let key in data) {
                        if (data[key].includes(dataColumn)) return true;
                    }
                    return false;
                }
            }).addTo(map)
        ];
    // } else {
    //     layers[0].setStyle(style(dataColumn));
    // }
}

function style(dataColumn) {
    return function(feature) {
        const data = feature.properties.data;

        if (data["1"] && data["1"].includes(dataColumn)) {
            return {
                fillOpacity: 0.6,
                stroke: false,
                fill: true,
                fillColor: "blue"
            };
        }

        if (data["-1"] && data["-1"].includes(dataColumn)) {
            return {
                fillOpacity: 0.6,
                stroke: false,
                fill: true,
                fillColor: "red"
            };
        }

        if (data["2"] && data["2"].includes(dataColumn)) {
            return {
                fillOpacity: 0.6,
                stroke: false,
                fill: true,
                fillColor: "purple"
            };
        }

        return {
            fillOpacity: 0.6,
            stroke: false,
            fill: false
        };
    };
}

var bbox = turf.bbox(ancPam);

var observer = new MutationObserver(function(mutations) {
    mutations.forEach(function(m) {
        m.addedNodes.forEach(function(n) {
            if (n.getElementsByClassName == null) return;

            var elements = n.getElementsByClassName("leaflet-map");
            Array.prototype.forEach.call(elements, function(element) {
                var map = L.map(element).fitBounds([
                    [bbox[1], bbox[0]], [bbox[3], bbox[2]]
                ]);
                L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    minZoom: 1,
                    maxZoom: 12
                }).addTo(map);
                maps[element._leaflet_id] = map;
                console.log("added leaflet id", element._leaflet_id);
                configureMap(element);
            });
        });

        m.removedNodes.forEach(function(n) {
            if (n.getElementsByClassName == null) return;

            var elements = n.getElementsByClassName("leaflet-map");
            Array.prototype.forEach.call(elements, function(element) {
                if (element._leaflet_id != null) {
                    console.log("removing map with leaflet id", element._leaflet_id);
                    maps[element._leaflet_id].remove();
                    maps[element._leaflet_id] = null;
                    mapLayers[element._leaflet_id] = null;
                }
            });
        });

        if (m.type == "attributes") {
            configureMap(m.target);
        }
    });
});

observer.observe(document.body, {
    subtree: true,
    childList: true,
    attributes: true,
    attributeFilter: ["data-map-column"],
    attributeOldValue: true
});

