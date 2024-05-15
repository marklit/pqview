#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=C0116 C0209 R0916 R0912 R0915 R0914 C0201 R1732 W1514

"""Analyse the file structure of Parquet files"""

from   collections       import Counter
from   functools         import reduce
from   operator          import add, itemgetter
from   os                import unlink
from   tempfile          import NamedTemporaryFile

import altair            as alt
from   altair_saver      import save
import humanize
import pandas            as pd
import pyarrow.parquet   as pq
from   pyecharts         import options as opts
from   pyecharts.charts  import Sunburst
from   tabulate          import tabulate
import typer


app = typer.Typer(rich_markup_mode='rich')


def render_table(out:dict):
    keys = out[0].keys()

    print(tabulate([[None if k not in x.keys() else x[k]
                     for k in keys]
                    for x in out],
                   headers=keys,
                   tablefmt='orgtbl',
                   floatfmt='.3f',
                   intfmt=','))


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

    sizes_ = \
        reduce(add,
               (map(Counter,
                    [{pf.metadata.row_group(rg).column(col).path_in_schema:
                        pf.metadata.row_group(rg).column(col).total_compressed_size}
                     for col in range(0, pf.metadata.num_columns)
                     for rg in range(0, pf.metadata.num_row_groups)])))

    sizes_ = dict(sorted(sizes_.items(), key=lambda x: x[1], reverse=True))

    for field, num_bytes in sizes_.items():
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
                    for country_name in inventories[continent_name]
                                            .keys()]) # pylint: disable=C0206
            for continent_name in inventories.keys()] # pylint: disable=C0206

    temp_ = NamedTemporaryFile(suffix='.html', delete=False)

    Sunburst(init_opts=opts.InitOpts(width='1000px',
                                     height='1000px'))\
    .add(series_name='',
         data_pair=data,
         radius=[0, '90%'])\
    .set_global_opts(title_opts=opts.TitleOpts(title=''))\
    .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}",
                                               font_size=24))\
    .set_dark_mode()\
    .render(temp_.name)

    html = open(temp_.name, 'r').read()
    unlink(temp_.name)
    return html


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

        for type_ in stats.keys(): # pylint: disable=C0206
            for col_name in stats[type_]:
                stats2.append({'type': type_,
                               'col_name': col_name,
                               'value': stats[type_][col_name]})

        html = render_sunburst(stats2,
                               parent_key='type',
                               child_key='col_name',
                               # WIP: Increase this to 10 MB by default
                               group_under=0)
        print(html)
    else:
        sizes_ = \
            reduce(add,
                   (map(Counter,
                        [{pf.metadata.row_group(rg).column(col).physical_type:
                            pf.metadata.row_group(rg).column(col).total_compressed_size}
                         for col in range(0, pf.metadata.num_columns)
                         for rg in range(0, pf.metadata.num_row_groups)])))

        sizes_ = dict(sorted(sizes_.items(), key=lambda x: x[1], reverse=True))

        for field, num_bytes in sizes_.items():
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

    ratios_ = []

    for rg in range(0, pf.metadata.num_row_groups):
        for col in range(0, pf.metadata.num_columns):
            x = pf.metadata.row_group(rg).column(col)
            ratio = x.total_compressed_size / x.total_uncompressed_size
            ratios_.append(r'%.1f' % ratio)

    sort_id = 0 if sort_key == 'ratio' else 1

    render_table([{'ratio': '%.1f:1' % (1 / float(ratio)), 'num_rg': num_rg}
                  for ratio, num_rg in sorted(Counter(ratios_).items(),
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


@app.command()
def ratios_by_column(pq_file:str):
    '''
    Compression ratios of each column by row-group
    '''
    pf = pq.ParquetFile(pq_file)

    cols = [col.path for col in pf.schema]

    # WIP: https://altair-viz.github.io/gallery/lasagna_plot.html
    values = pd.DataFrame([
        {'rowgroup': rg,
         'column': cols[col],
         'efficiency':
            pf.metadata.row_group(rg).column(col).total_compressed_size /
            pf.metadata.row_group(rg).column(col).total_uncompressed_size}
       for rg in range(0, pf.metadata.num_row_groups)
       for col in range(0, pf.metadata.num_columns)])

    color_condition = alt.condition(
        "datum.rowgroup == 1 && datum.rowgroup == 1",
        alt.value("black"),
        alt.value(None),
    )

    alt.renderers.set_embed_options(theme='dark')

    chart = alt.Chart(values, width=800, height=800).mark_rect().encode(
        alt.X("rowgroup:N")
            .title("Row-group")
            .axis(
                #format="%d",
                labelAngle=0,
                labelOverlap=False,
                labelColor=color_condition,
                tickColor=color_condition,
            ),
        alt.Y("column:N").title('Column Name'),
        alt.Color("efficiency").title("Efficiency")
    )

    temp_ = NamedTemporaryFile(suffix='.html', delete=False)
    save(chart, temp_.name)
    print(open(temp_.name, 'r').read())
    unlink(temp_.name)


if __name__ == "__main__":
    app()
