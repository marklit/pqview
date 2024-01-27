#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from collections import Counter
from functools   import reduce
from glob        import glob
from operator    import add, itemgetter

import humanize
import pyarrow.parquet as pq
from   pyecharts        import options as opts
from   pyecharts.charts import HeatMap, Sunburst
from   tabulate         import tabulate
import typer


app = typer.Typer(rich_markup_mode='rich')


def render_table(out:dict):
    keys = out[0].keys()

    out2 = []

    for x in out:
        out2.append([None if k not in x.keys() else x[k] for k in keys])

    print(tabulate(out2,
                   headers=keys,
                   tablefmt='orgtbl',
                   floatfmt='.3f',
                   #intfmt=',',
                   ))


@app.command()
def overall(pq_file:str):
    '''
    Statistics on the overall file
    '''
    pf = pq.ParquetFile(pq_file)
    print(pf.metadata)


@app.command()
def row_groups(pq_file:str, sort_key:str='row-groups', reverse:bool=True):
    '''
    Number of records per row-group
    '''
    pf = pq.ParquetFile(pq_file)

    sort_id = 1 if sort_key == 'row-groups' else 0

    render_table([{'num_records': num_records, 'num_rg': num_rg}
                  for num_records, num_rg in sorted(Counter(pf.metadata.row_group(rg).num_rows
              for rg in range(0, pf.metadata.num_row_groups)).items(),
                                              key=itemgetter(sort_id),
                                              reverse=reverse)])

@app.command()
def schemes(pq_file:str):
    '''
    Every compression scheme used
    '''
    pf = pq.ParquetFile(pq_file)
    print(', '.join(set(pf.metadata.row_group(rg).column(col).compression
                        for col in range(0, pf.metadata.num_columns)
                        for rg in range(0, pf.metadata.num_row_groups))))




@app.command()
def sizes(pq_file:str):
    '''
    Disk space consumption of each column in compressed form
    '''
    pf = pq.ParquetFile(pq_file)

    sizes = \
        reduce(add,
               (map(Counter,
                    [{pf.metadata.row_group(rg).column(col).path_in_schema:
                        pf.metadata.row_group(rg).column(col).total_compressed_size}
                     for col in range(0, pf.metadata.num_columns)
                     for rg in range(0, pf.metadata.num_row_groups)])))

    sizes = {k: v for k, v in sorted(sizes.items(),
                                     key=lambda x: x[1],
                                     reverse=True)}

    for field, num_bytes in sizes.items():
        print('%10s %s' % (humanize.naturalsize(num_bytes), field))


def render_sunburst(source:list,
                    parent_key='continent_name',
                    child_key='country_name',
                    group_under=20_000_000):
    inventories = {}

    for rec in source:
        if rec[parent_key] not in inventories.keys():
            inventories[rec[parent_key]] = {}

        country_name = rec[child_key] \
                            if rec['value'] > group_under else 'Other'

        if country_name in inventories[rec[parent_key]].keys():
            inventories[rec[parent_key]][country_name] = \
                inventories[rec[parent_key]]\
                           [country_name] + rec['value']
        else:
            inventories[rec[parent_key]]\
                       [country_name] = rec['value']

    data = [opts.SunburstItem(
                 name=continent_name,
                 children=[
                    opts.SunburstItem(
                        name=country_name,
                        value=inventories[continent_name]
                                         [country_name])
                    for country_name in inventories[continent_name].keys()])
            for continent_name in inventories.keys()]

    Sunburst(init_opts=opts.InitOpts(width='1000px',
                                     height='1000px'))\
    .add(series_name='',
         data_pair=data,
         radius=[0, '90%'])\
    .set_global_opts(title_opts=opts.TitleOpts(title=''))\
    .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}",
                                               font_size=24))\
    .set_dark_mode()\
    .render('sunburst.html')


@app.command()
def types(pq_file:str, html:bool=False):
    '''
    Disk space consumption of each data type in compressed form
    '''
    pf = pq.ParquetFile(pq_file)

    if html:
        col_names = [col.path for col in pf.schema]
        stats = {}

        for col in range(0, pf.metadata.num_columns):
            for rg in range(0, pf.metadata.num_row_groups):
                pt = pf.metadata.row_group(rg).column(col).physical_type

                if pt not in stats.keys():
                    stats[pt] = {}

                if col_names[col] not in stats[pt].keys():
                    stats[pt][col_names[col]] = 0

                stats[pt][col_names[col]] = \
                    stats[pt][col_names[col]] + \
                    pf.metadata.row_group(rg).column(col).total_compressed_size

        stats2 = []

        for type_ in stats.keys():
            for col_name in stats[type_]:
                stats2.append({'type': type_,
                               'col_name': col_name,
                               'value': stats[type_][col_name]})

        render_sunburst(stats2,
                    parent_key='type',
                    child_key='col_name',
                    group_under=0)

    else:
        sizes = \
            reduce(add,
                   (map(Counter,
                        [{pf.metadata.row_group(rg).column(col).physical_type:
                            pf.metadata.row_group(rg).column(col).total_compressed_size}
                         for col in range(0, pf.metadata.num_columns)
                         for rg in range(0, pf.metadata.num_row_groups)])))

        sizes = {k: v for k, v in sorted(sizes.items(),
                                         key=lambda x: x[1],
                                         reverse=True)}

        for field, num_bytes in sizes.items():
            print('%10s %s' % (humanize.naturalsize(num_bytes), field))


@app.command()
def most_compressed(pq_file:str, min_size:int=0):
    '''
    Most-compressed column in any one row-group
    '''
    pf = pq.ParquetFile(pq_file)

    lowest_val, lowest_rg, lowest_col = None, None, None

    for rg in range(0, pf.metadata.num_row_groups):
        for col in range(0, pf.metadata.num_columns):
            x = pf.metadata.row_group(rg).column(col)

            if min_size and min_size > x.total_uncompressed_size:
                continue

            ratio = x.total_compressed_size / x.total_uncompressed_size

            if not lowest_val or lowest_val > ratio:
                lowest_val = ratio
                lowest_rg, lowest_col = rg, col

    rg_column = pf.metadata.row_group(lowest_rg).column(lowest_col)

    print('From %s to %s (ratio of %d:1)' % (
        humanize.naturalsize(rg_column.total_uncompressed_size),
        humanize.naturalsize(rg_column.total_compressed_size),
        rg_column.total_uncompressed_size /
        rg_column.total_compressed_size))
    print()
    print(rg_column)


@app.command()
def ratios(pq_file:str, sort_key:str='ratio', reverse:bool=False):
    '''
    Compression ratios of each column by row-group
    '''
    pf = pq.ParquetFile(pq_file)

    ratios = []

    for rg in range(0, pf.metadata.num_row_groups):
        for col in range(0, pf.metadata.num_columns):
            x = pf.metadata.row_group(rg).column(col)
            ratio = x.total_compressed_size / x.total_uncompressed_size
            ratios.append('%.1f' % ratio)

    sort_id = 0 if sort_key == 'ratio' else 1

    render_table([{'ratio': ratio, 'num_rg': num_rg}
                  for ratio, num_rg in sorted(Counter(ratios).items(),
                                              key=itemgetter(sort_id),
                                              reverse=reverse)])

@app.command()
def minmax(pq_file:str, column:str):
    '''
    Minimum and maximum values of a column's row-groups
    '''
    pf = pq.ParquetFile(pq_file)

    col_num = [col_num
               for col_num, col in enumerate(pf.schema)
               if col.path == column][0]

    render_table(
        [{'rg_num': rg,
         'min': pf.metadata.row_group(rg).column(col_num).statistics.min,
         'max': pf.metadata.row_group(rg).column(col_num).statistics.max}
         for rg in range(0, pf.metadata.num_row_groups)])

'''
Chart types:

* Sunburst with break up by column dots (bbox would have 4 children)

    1.2 GB geometry
  189.8 MB id
  121.4 MB bbox.maxy
  121.3 MB bbox.miny
  119.7 MB bbox.maxx
  119.6 MB bbox.minx
   82.6 MB sources.list.element.recordId
   10.7 MB updateTime
    2.9 MB height
    2.6 MB sources.list.element.confidence
    2.1 MB names.common.list.element.value
    1.1 MB sources.list.element.dataset
  584.1 kB class

* Sunburst of space usage by column type with children being column names
'''

@app.command()
def ratios_by_column(pq_file:str):
    '''
    Compression ratios of each column by row-group
    '''
    pf = pq.ParquetFile(pq_file)

    cols = [col.path for col in pf.schema]

    values = [[rg,
              cols[col],
              pf.metadata.row_group(rg).column(col).total_compressed_size /
              pf.metadata.row_group(rg).column(col).total_uncompressed_size]
             for rg in range(0, pf.metadata.num_row_groups)
             for col in range(0, pf.metadata.num_columns)]

    min_val = min([x for _, _, x in values])
    max_val = max([x for _, _, x in values])

    values = [[rg, col, 100 - (100 * ((val - min_val)/(max_val - min_val)))]
              for rg, col, val in values]

    c = (
        HeatMap()
        .add_xaxis([rg for rg in range(0, pf.metadata.num_row_groups)])
        .add_yaxis(
            "compression ratio",
            cols,
            values,
            label_opts=opts.LabelOpts(is_show=False, position="inside"),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="Compression Ratio HeatMap"),
            visualmap_opts=opts.VisualMapOpts(),
        )
        .render("heatmap_with_label_show.html")
    )


if __name__ == "__main__":
    app()