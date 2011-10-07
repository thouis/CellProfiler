"""sqlite_project.py - the sqlite backend for a project

This module models a CellProfiler project

CellProfiler is distributed under the GNU General Public License,
but this file is licensed under the more permissive BSD license.
See the accompanying file LICENSE for details.

Copyright (c) 2003-2009 Massachusetts Institute of Technology
Copyright (c) 2009-2011 Broad Institute
Copyright (c) 2011 Institut Curie
All rights reserved.

Please see the AUTHORS file for credits.

Website: http://www.cellprofiler.org
"""

__version__ = "$Revision: 1 $"

import sqlite3
import uuid

schema = """
/*
 * CellProfiler project schema
 *
 * A note on foreign keys: if table foo refers to bar.baz as a foreign
 * key, it should be named foo.bar_baz so that the typical comparisons look like this:
 *    ... where foo.bar_baz = bar.baz ...
 */
 
 /*
  * project table
  *
  * This has one record that stores globally relevant values
  * for the project.
  */
create table project (
    creation_time varchar(30) not null default(datetime()),
    modification_time varchar(30),
    version integer not null default(1)
);
 
/*
 * Every image has a record in this table. The image_id
 * is used throughout the schema to point at the URL.
 */
create table image (
    id integer not null primary key,
    url text not null);
create unique index image_idx on image(url);

/*
 * The directory table stores directory names in an agnostic fashion.
 * Its purpose is to provide the directory structure to the UI. There
 * is no explicit link between URLs and directories.
 *
 * The parent links directories in the hierarchy.
 */
create table directory (
    id integer not null primary key,
    name text not null,
    parent_id integer,
    constraint directory_parent_fk foreign key (parent_id)
        references directory(id));
create unique index directory_name_idx on directory(name);
create index directory_parent_idx on directory(parent_id);

/*
 * Metadata is key/value pairs. I (leek) have normalized the storage
 * so that each metadata key has an identifier, so that the possible values
 * associated with a key are explicitly listed and so that a key/value pair
 * has its own ID. This has the advantage of reducing the text in a
 * key and value to a single number at the cost of a few joins and it localizes
 * some important indices in some of the shorter tables.
 */
create table metadata_key(
    id integer not null primary key,
    key text not null);
create unique index metadata_key_idx on metadata_key(key);

create table metadata_value(
    id integer not null primary key,
    metadata_key_id integer not null,
    value text not null,
    constraint metadata_key_id_fk foreign key (metadata_key_id)
        references metadata_key(id));
create unique index metadata_value_idx on metadata_value(metadata_key_id, value);

create table metadata(
    metadata_value_id integer not null,
    image_id integer not null,
    constraint metadata_pk primary key (metadata_value_id, image_id),
    constraint metadata_value_fk foreign key (metadata_value_id)
        references metadata_value(id),
    constraint metadata_image_fk foreign key (image_id)
        references image(id));
create index metadata_image_idx on metadata(image_id);

/*
 * Urlsets are subsets of the urls in the image table
 */
create table urlset (
    id integer not null primary key,
    name text not null);
create unique index urlset_name_idx on urlset(name);

create table urlset_image (
    urlset_id integer not null,
    image_id integer not null,
    constraint urlset_pk primary key (urlset_id, image_id),
    constraint urlset_imageset_fk foreign key (urlset_id)
        references urlset(id),
    constraint urlset_image_image_fk foreign key (image_id)
        references image(id)
    );
create index urlset_image_idx on urlset_image(image_id);

/*
 * Image sets are rectangular with the rows being defined by one or
 * more metadata key values and the columns defined by a column
 * metadata key value.
 *
 * One "gotcha" here is that someone might create an image set and then
 * re-annotate their images with new metadata. When you create an image set,
 * the metadata for each image number is frozen and when you put frames into
 * the set, the reason for their association with the row is not recorded
 * on purpose.
 *
 * The image_set table's purpose is mostly to have unique image set names
 * but it could contain documentation too.
 */
create table imageset (
    id integer not null primary key,
    name text not null
);
create unique index imageset_name_idx on imageset(name);

/*
 * imageset_key saves the metadata keys that were used to create the image
 * set. The "sort_order" field gives the placement of the key in the sort
 * order (for instance, "Plate" = 1, "Well" = 2, "Site" = 3)
 */
create table imageset_key (
    id integer not null primary key,
    imageset_id text not null,
    metadata_key_id not null,
    sort_order integer not null,
    constraint imageset_key_id_fk foreign key (imageset_id)
        references imageset(id),
    constraint imageset_key_key_fk foreign key (metadata_key_id)
        references metadata_key(id));
create unique index imageset_key_idx on imageset_key(imageset_id, metadata_key_id);
create unique index imageset_key_sort_idx on imageset_key(imageset_id, sort_order);

/*
 * channel saves the metadata key/value pairs used to define
 * each channel in the imageset. The metadata_value_id_fk, if present joins to
 * the metadata value that was used when collecting the images in the
 * channel. The channel name can be different, though, or the images
 * in the channel might have been collected using some mechanism other
 * than metadata.
 */
create table channel (
    id integer not null primary key,
    imageset_id integer not null,
    metadata_value_id integer,
    name text not null,
    constraint channel_imageset_fk foreign key (imageset_id)
        references imageset(id),
    constraint channel_fk foreign key (metadata_value_id)
        references metadata_value(id));
create unique index channel_idx on channel(imageset_id, metadata_value_id);
create unique index channel_name_idx on channel(imageset_id, name);

/*
 * imageset_row numbers the rows in each image set
 */
create table imageset_row (
    id integer not null primary key,
    imageset_id integer not null,
    image_number integer not null,
    constraint imageset_row_imageset_id_fk foreign key (imageset_id)
        references imageset(id));
create unique index imageset_row_idx on imageset_row (imageset_id, image_number);

/*
 * imageset_metadata saves the metadata values associated with each
 * row in an image set. Each row documents the value for one key.
 */
create table imageset_metadata (
    imageset_row_id integer not null,
    metadata_value_id integer not null,
    constraint imageset_metadata_pk primary key (imageset_row_id, metadata_value_id),
    constraint imageset_metadata_row_id_fk foreign key (imageset_row_id)
        references imageset_row(id),
    constraint imageset_metadata_metadata_id_fk foreign key (metadata_value_id)
        references metadata_value(id));
        
/*
 * imageset_image collects the images associated with each imageset row
 * and with each imageset channel
 */
create table imageset_image (
    imageset_row_id integer not null,
    image_id integer not null,
    channel_id integer not null,
    constraint imageset_image_pk primary key (imageset_row_id, image_id, channel_id),
    constraint imageset_image_row_fk foreign key (imageset_row_id)
        references imageset_row(id),
    constraint imageset_image_image_fk foreign key (image_id)
        references image(id),
    constraint imageset_image_channel_fk foreign key (channel_id)
        references channel(id));
create index imageset_image_idx on imageset_image(image_id);
"""

class SQLiteProject(object):
    def __init__(self, path):
        '''Create a project, using a project backend'''
        self.connection = sqlite3.connect(path)
        self.cursor = self.connection.cursor()
        self.cursor.execute("""
        select count('x') from sqlite_master
         where tbl_name = 'project' and type='table'""")
        if list(self.cursor)[0][0] == 0:
            for statement in schema.split(";"):
                self.cursor.execute(statement)
        self.cursor.execute("begin transaction")
        
    def close(self):
        self.connection.close()
        
    def commit(self):
        '''Commit any changes'''
        self.connection.commit()
        self.cursor.execute("begin transaction")
        
    def rollback(self):
        self.connection.rollback()
        self.cursor.execute("begin transaction")
        
    def add_url(self, url):
        '''Add a URL to the list of image files
        
        url - url of an image file
        
        returns the image_id for the URL
        '''
        self.cursor.execute("insert into image (url) values (?)", [url])
        return self.cursor.lastrowid
        
    def get_url_image_id(self, url):
        '''Get the image ID for a url'''
        self.cursor.execute("select max(id) from image where url=?", [url])
        result = self.cursor.fetchall()
        return None if len(result) == 0 else result[0][0]
    
    def get_url(self, image_id):
        '''Get a URL given its image id
        
        image_id - the image ID for a url, can be a sequence
                   in which case, we return a sequence of urls
        '''
        self.cursor.execute("select max(url) from image where id=?", 
                            [ image_id ])
        result = self.cursor.fetchall()
        return None if len(result) == 0 else result[0][0]
    
    def remove_url_by_id(self, image_id):
        '''Remove a URL, using its image id'''
        self.cursor.execute("delete from image where id=?", [ image_id ])
        
    def add_directory(self, name, parent=None):
        '''Add a directory to the project
        
        Put a directory into the directory table, optionally linking it
        to its parent.
        
        name - the directory's URL
        
        parent - the name of the parent directory.
        '''
        if parent is None:
            self.cursor.execute("""
            insert into directory (name)
            select ? as name
            where not exists (select 'x' from directory where name=?)
            """, [name, name])
        else:
            self.cursor.execute("""
            insert into directory (name, parent_id)
            select ? as name, d.id from directory d
            where d.name = ?
            and not exists (select 'x' from directory where name = ?)""",
            [name, parent, name])
            
    def get_directories(self):
        '''Return all directories in the project
        '''
        self.cursor.execute("select name from directory order by name")
        return [x[0] for x in self.cursor]
    
    def get_root_directories(self):
        '''Return all root directories in the project
        '''
        self.cursor.execute(
            "select name from directory where parent_id is null order by name")
        return [x[0] for x in self.cursor]

    def get_subdirectories(self, parent):
        '''Return all immediate subdirectories of the parent
        
        parent - the name of the parent directory
        '''
        self.cursor.execute(
            """select directory.name from directory 
                 join directory pd on pd.id = directory.parent_id
                where pd.name = ?
                order by directory.name
            """, [parent])
        return [x[0] for x in self.cursor]
        
    def remove_directory(self, name):
        '''Remove a directory and its subdirectories
        
        name - name of the directory to remove
        
        Note: does not remove the URLs "in" the directory.
        '''
        for subname in self.get_subdirectories(name):
            self.remove_directory(subname)
        self.cursor.execute("delete from directory where name = ?", [name])
    
    def get_metadata_keys(self):
        '''Return all the metadata keys for this project
        
        returns a sequence of key names
        '''
        self.cursor.execute("select key from metadata_key")
        return [x[0] for x in self.cursor]
        
    def get_metadata_values(self, key):
        '''Return the metadata values that have been assigned to a particular key
        
        key - a metadata key.
        '''
        self.cursor.execute("""
        select metadata_value.value 
          from metadata_key
          join metadata_value on metadata_value.metadata_key_id = metadata_key.id
          where metadata_key.key = ? order by metadata_value.value""", [key])
        return [x[0] for x in self.cursor]
    
    def add_image_metadata(self, keys, values, image_id):
        '''Assign metadata values to image planes
        
        keys - a sequence of the metadata keys to be assigned
        
        values - either a sequence of values for the keys or, if being assigned
                to multiple images, an N x M array of values where N is
                the number of images and M is the number of keys
                
        image_id - the image_id of the image or a 1-d array of image_ids
        '''
        #
        # Standardize the code by converting image_id into an array
        #
        if not hasattr(image_id, "__iter__"):
            image_id = [image_id]
            values = [values]
        #
        # Get the key_id for each key
        #
        key_ids = [ self.add_metadata_key(key) for key in keys]
        #
        # Get the value ids for each value
        #
        value_ids = [[self.add_metadata_value(key_id, value) 
                      for key_id, value in zip(key_ids, value_row)]
                     for value_row in values]
        #
        # Delete existing records for the image and key
        #
        self.cursor.executemany(
            """delete from metadata
                where image_id = ? and
               exists (select 'x' from metadata_value v
                        where v.id = metadata_value_id and metadata_key_id = ?)""",
            [(i, key) for i in image_id for key_id in key_ids])
        #
        # Create new records
        #
        self.cursor.executemany(
            """insert into metadata (metadata_value_id, image_id)
               values (?, ?)""",
            sum([[(value_id, i) for value_id in value_id_row]
                 for value_id_row, i in zip(value_ids, image_id)], []))
    
    def add_metadata_key(self, key):
        '''Add a key to the metadata key table
        
        key - key to add
        
        adds the key if not present, returns the key id.
        '''
        self.cursor.execute("select id from metadata_key where key = ?",
                            [key])
        result = self.cursor.fetchall()
        if len(result) > 0:
            return result[0][0]
        else:
            self.cursor.execute("insert into metadata_key (key) values (?)",
                                [key])
            return self.cursor.lastrowid
        
    def add_metadata_value(self, key_id, value):
        '''Add a key/value pair to the metadata_value table
        
        key_id - the id for the key in the metadata_key table
        
        value -  the value to add
        
        returns the metadata_value id
        '''
        self.cursor.execute(
            """select id from metadata_value
            where metadata_key_id = ? and value = ?""", [key_id, value])
        result = self.cursor.fetchall()
        if len(result) > 0:
            return result[0][0]
        else:
            self.cursor.execute(
                """insert into metadata_value (metadata_key_id, value)
                values (?,?)""", [key_id, value])
            return self.cursor.lastrowid
            
    def remove_image_metadata(self, key, image_id):
        '''Remove metadata values from image planes
        
        key - a key to be removed from a single image's metadata or from
              a sequence of images
         
        image_id - the image_id of the image or a 1-d array of image ids
        '''
        if not hasattr(image_id, "__iter__"):
            image_id = [image_id]
        self.cursor.executemany(
            """delete from metadata
                where image_id = ?
                  and exists (select 'x'
                                from metadata_value
                                join metadata_key on metadata_value.metadata_key_id = metadata_key.id
                               where metadata_value_id = metadata_value.id and metadata_key.key = ?)""",
            [(i, key) for i in image_id])
        
    def get_image_metadata(self, image_id):
        '''Get all metadata key/value pairs for an image
        
        image_id - the image's image_id
        
        returns a dictionary of metadata key/value pairs for the image
        '''
        self.cursor.execute(
            """select metadata_key.key, metadata_value.value
                 from metadata
                 join metadata_value on metadata.metadata_value_id = metadata_value.id
                 join metadata_key on metadata_value.metadata_key_id = metadata_key.id
                where metadata.image_id = ?
            """, [image_id])
        return dict([(k, v) for k,v in self.cursor])

    def get_images_by_metadata(self, keys, values=None, urlset=None):
        '''Return images by metadata key and value
        
        keys - the metadata keys for the operation
        values - if None, return images, ordered by value.
                 if a single tuple of values, return images matching
                 those values for the keys.
        urlset - if None, match any images, if not None, match only
                 images in the urlset
        
        returns an array where each row represents a frame. For M keys,
        the first M values of the array are the metadata values for
        each of the keys for that row's frame. The last value in each
        row of the array is the image_id of the image.
        '''
        if len(keys) == 0:
            # Sort of cheating. Get everything.
            if urlset is None:
                self.cursor.execute("select id from image order by url")
            else:
                self.cursor.execute("""
                select urlset_image.image_id
                  from urlset_image
                  join image on urlset_image.image_id = image.id
                 where urlset_image.name = ?
                 order by image.url""", [urlset])
            return [x for x in self.cursor]
            
        key_table_aliases = ["k%d" % (i+1) for i in range(len(keys))]
        value_table_aliases = ["v%d" % (i+1) for i in range(len(keys))]
        fields = []
        parameters = list(keys)
        join = "image "
        where = " and ".join(["%s.key=?" % a for a in key_table_aliases])
        #
        # Handle each of the key/value pairs
        #
        for i, k in enumerate(keys):
            fields.append("%s.value" % value_table_aliases[i])
            join += " join metadata m%d on m%d.image_id = image.id" % (i,i)
            join += (" join metadata_value %s on %s.id = m%d.metadata_value_id" % 
                     (value_table_aliases[i], value_table_aliases[i], i))
            join += (" join metadata_key %s on %s.id = %s.metadata_key_id" %
                     (key_table_aliases[i], key_table_aliases[i],
                      value_table_aliases[i]))
            if values is not None:
                where += " and %s.value = ?" % value_table_aliases[i]
                parameters.append(values[i])
        #
        # If there is a urlset, handle that too
        #
        if urlset is not None:
            join += " join urlset_image on urlset_image.image_id = image.id"
            join += " join urlset on  urlset_image.urlset_id = urlset.id"
            where += " and urlset.name = ?"
            parameters.append(urlset)
        fields.append("image.id as image_id")
        statement = "select " + ",".join(fields)
        statement += " from " + join + " where " + where
        self.cursor.execute(statement, parameters)
        return [[ x for x in row] for row in self.cursor]
    
    def make_urlset(self, name):
        '''Create a urlset with a given name
        
        A urlset is a collection of image URLs.
        Sometimes, you might want to only run a pipeline on some
        of the images in the dataset (for instance, illumination correction
        images or one plate of images). So image set operations take
        urlsets to allow this sort of flexibility.
        
        name - the name of the urlset
        '''
        self.cursor.execute("insert into urlset (name) values (?)", [name])
        return self.cursor.lastrowid
        
    def get_urlset_names(self):
        '''Return the names of all of the urlsets'''
        self.cursor.execute("select name from urlset")
        return [row[0] for row in self.cursor]
    
    def remove_urlset(self, name):
        '''Delete a urlset
        
        name - the name of the urlset to delete
        '''
        self.cursor.execute(
            """delete from urlset_image where exists
               (select 'x' from urlset u where u.id = urlset_image.urlset_id
                   and u.name = ?)""", [name])
        self.cursor.execute("delete from urlset where name = ?", [name])
    
    def add_images_to_urlset(self, name, image_ids):
        '''Add images to a urlset
        
        name - the name of the urlset
        
        image_ids - a 1-d array of image ids to add to the urlset
        '''
        self.cursor.executemany("""
        insert into urlset_image (urlset_id, image_id)
        select distinct urlset.id as urlset_id, image.id as image_id
          from urlset, image
         where urlset.name = ? and image.id = ?
           and not exists (select 'x' from urlset_image
                            where urlset_image.urlset_id = urlset.id and urlset_image.image_id = image.id)""",
                            [(name, i) for i in image_ids])
        
    def remove_images_from_urlset(self, name, image_ids):
        '''Remove frames from a urlset
        
        name - the name of the urlset

        image_ids - a 1-d array of image ids to add to the urlset
        '''
        self.cursor.executemany("""
        delete from urlset_image where image_id = ?
           and exists (select 'x' from urlset u 
                        where u.id = urlset_id
                          and u.name = ?)""", [(i, name) for i in image_ids])
        
    def get_urlset_members(self, name):
        '''Return all images in the urlset
        
        Returns a 1-d array of the image ids in the urlset
        '''
        self.cursor.execute("""
        select urlset_image.image_id 
          from urlset_image join urlset on urlset_image.urlset_id = urlset.id
         where urlset.name = ?""", [name])
        return [r[0] for r in self.cursor]
    
    def create_imageset(self, name, keys, channel_key,
                        channel_values = None,
                        channel_names = None, urlset=None):
        '''Create an image set
        
        name - the name of the image set
        
        urlset - the name of the urlset. If None, operate on all frames
        
        keys - the metadata keys that uniquely define an image set row
        
        channel_key - the metadata key that assigns a frame to a channel
        
        channel_values - the channels to add to the image set. For instance,
        if the channel_key is "wavelength" and you only want "w1" and "w2"
        in the imageset, but not "w3", channel_values would be ["w1", "w2"].
        If None, accept all channel values.
        
        channel_names - names matching each channel value. If None, use
        the channel value as the channel name.
        
        Create an image set where each row in the image set has unique values
        for the set of metadata keys. For instance, the keys might be
        "Plate", "Well" and "Site" and a row might have values "P-12345",
        "A05" and "s3". Each (conceptual) row has columns which are the
        possible values for the channel key in the  urlset. For instance,
        the channel_key might be "Wavelength" with values "w1" and "w2".
        
        The result of the operation is an image set whose rows can be referenced
        by image number or by key values.
        '''
        #
        # create the imageset record
        #
        self.cursor.execute(
            "insert into imageset (name) values (?)", [name])
        imageset_id = self.cursor.lastrowid
        #
        # create the imageset_key records
        #
        self.cursor.executemany(
            """insert into imageset_key (imageset_id, metadata_key_id, sort_order)
               select ? as imageset_id, metadata_key.id as metadata_key_id, ? as sort_order
                 from metadata_key where metadata_key.key = ?""",
            [(imageset_id, i, key) for i, key in enumerate(keys)])

        if urlset is not None:
            urlset_join = " join urlset_image on urlset_image.image_id = image.id\n"
            urlset_join += " join urlset on urlset_image.urlset_id = urlset.id\n"
            urlset_clause = " and urlset.name = ? "
            urlset_where = " where urlset.name = ? "
            urlset_params = [urlset]
        else:
            urlset_join = ""
            urlset_clause = ""
            urlset_where = ""
            urlset_params = []
        #
        # if the values are given, create the channels rows now
        #
        if channel_values is not None:
            if channel_names is None:
                channel_names = channel_values
            self.cursor.executemany(
                """insert into channel (imageset_id, metadata_value_id, name)
                select ? as imageset_id, metadata_value.id as metadata_value_id, ? as name
                  from metadata_key join metadata_value on metadata_value.metadata_key_id = metadata_key.id
                 where metadata_key.key = ? and metadata_value.value = ?""",
                [(imageset_id, name, channel_key, value)
                 for name, value in zip(channel_names, channel_values)])
            # XXX - URLSET?
        else:
            #
            # Otherwise, we have to select all images that have values
            # for the channel key and the other keys, possibly joining
            # to the urlset.
            #
            statement = "insert into channel (imageset_id, metadata_value_id, name)\n"
            statement += " select distinct ? as imageset_id, \n"
            statement += "                 metadata_value.id as metadata_value_id,\n"
            statement += "                 metadata_value.value as name\n"
            statement += "  from image join metadata on metadata.image_id = image.id\n"
            statement += "  join metadata_value on metadata.metadata_value_id = metadata_value.id\n"
            statement += "  join metadata_key on metadata_value.metadata_key_id = metadata_key.id\n"
            statement += urlset_join
            statement += " where metadata_key.key = ? "
            statement += urlset_clause
            self.cursor.execute(statement, [imageset_id, channel_key] + urlset_params)
        #
        # Only three more tables to go.
        # The simplest way to do all this is to create a temporary
        # table that has the information properly ordered. If we are
        # very lucky, the rowid of the table can be used as the image_number.
        #
        # Create a temporary table with all of the metadata_value_ids of the
        # grouping metadata.
        temp = "T_" + uuid.uuid4().get_hex()
        statement = "create temporary table %s as select distinct \n" % temp
        statement += " ,".join(["mv%d.id as mv%d_id" % (idx, idx) for idx in range(len(keys))])
        statement += "\n from image \n"
        for idx in range(len(keys)):
            statement += "  join metadata m%d on m%d.image_id = image.id\n" % (idx, idx)
            statement += "    join metadata_value mv%d on m%d.metadata_value_id = mv%d.id\n" % (idx, idx, idx)
            statement += "    join metadata_key mk%d on mv%d.metadata_key_id = mk%d.id\n" % (idx, idx, idx)
        statement += "  join metadata mdc on mdc.image_id = image.id \n"
        statement += "  join channel on channel.metadata_value_id = mdc.metadata_value_id\n "
        statement += urlset_join
        clauses = ["channel.imageset_id = ?\n"] + ["mk%d.key = ? " % idx for idx in range(len(keys))]
        statement += " where " + " and ".join(clauses) + urlset_clause + "\n"
        self.cursor.execute(statement, [imageset_id] + keys + urlset_params)

        #
        # Create the imageset_row table from the temporary
        #
        self.cursor.execute("""
        insert into imageset_row (imageset_id, image_number)
        select ?, rowid from %s""" % temp, [imageset_id])  # note the arguments are reversed into the SQL
        #
        # Create the imageset_metadata for each metadata key
        #
        for idx in range(len(keys)):
            self.cursor.execute("""
            insert into imageset_metadata (imageset_row_id, metadata_value_id)
            select imageset_row.id, temp.mv%d_id
            from %s temp join imageset_row on temp.rowid = imageset_row.image_number
            where imageset_row.imageset_id = ?""" % (idx, temp), [imageset_id])
        #
        # Finally, select the images for all of the channels.
        #
        statement = """insert into imageset_image (imageset_row_id, image_id, channel_id)
                       select imageset_row.id as imageset_row_id,
                              image.id as image_id,
                              channel.id as channel_id \n"""
        statement += " from %s t join imageset_row on t.rowid = imageset_row.image_number \n" % temp
        statement += " join image "
        for idx in range(len(keys)):
            # Example: join metadata mv0 on t.mv0_id = mv0.metadata_value_id and mv0.image_id = image.id
            statement += "   join metadata mv%d on t.mv%d_id = mv%d.metadata_value_id\n" % (idx, idx, idx)
            statement += "     and mv%d.image_id = image.id\n" % (idx)
        statement += " join metadata mdc on mdc.image_id = image.id \n"
        statement += " join channel on channel.metadata_value_id = mdc.metadata_value_id where channel.imageset_id = ?"
        self.cursor.execute(statement, [imageset_id])
        self.cursor.execute("drop table %s" % temp)
        
    def remove_imageset(self, name):
        '''Delete the named imageset'''
        self.cursor.execute(
            """delete from imageset_image
               where exists (select 'x' from imageset_row r
               join imageset i on i.id = r.imageset_id
               where r.id = imageset_row_id and i.name = ?)""", [name])
        self.cursor.execute(
            """delete from imageset_metadata
               where exists (select 'x' from imageset_row r
               join imageset i on i.id = r.imageset_id
               where r.id = imageset_row_id and i.name = ?)""", [name])
        self.cursor.execute(
            """delete from imageset_row
               where exists (select 'x' from imageset i 
               where imageset_id = i.id and i.name = ?)""", [name])
        self.cursor.execute(
            """delete from channel
               where exists (select 'x' from imageset i 
               where imageset_id = i.id and i.name = ?)""", [name])
        self.cursor.execute(
            """delete from imageset_key
               where exists (select 'x' from imageset i 
               where imageset_id = i.id and i.name = ?)""", [name])
        self.cursor.execute(
            """delete from imageset where name = ?""", [name])
        
    def get_imageset_names(self):
        '''Get the names of the imagesets in the project'''
        self.cursor.execute("select name from imageset order by name")
        return [x[0] for x in self.cursor]
                
    def get_imageset_row_count(self, name):
        '''Return the number of rows in the named imageset'''
        self.cursor.execute(
            """select count('x') from imageset
               join imageset_row on imageset_row.imageset_id = imageset.id
               where imageset.name = ?""", [name])
        return list(self.cursor)[0][0]
        
    def get_imageset_row_images(self, name, image_number):
        '''Return the images in an imageset row
        
        name - the name of the imageset
        image_number - the one-based image number for the row
        
        returns a dictionary whose key is channel value and whose value
        is a 1-d array of image_ids for that row. An array might be empty for 
        some channel value (missing image) or might contain more than one
        image id (duplicate images).
        '''
        #
        # There might be a way to get null urls through outer joins
        # using subselects, but the path to that might be a little
        # inefficient. Querying the list of channels each time is
        # simple and not too inefficient.
        #
        self.cursor.execute("""
        select channel.name
         from imageset join channel on imageset.id = channel.imageset_id
         where imageset.name = ?""", [name])
        channels = [x[0] for x in self.cursor]
        
        self.cursor.execute("""
        select channel.name, image.id
         from imageset
         join channel on channel.imageset_id = imageset.id
         join imageset_row on imageset_row.imageset_id = imageset.id
         join imageset_image on imageset_image.imageset_row_id = imageset_row.id
         join image on imageset_image.image_id = image.id
        where imageset.name = ? and imageset_row.image_number = ?
          and imageset_image.channel_id = channel.id
        """, [name, image_number])
        result = dict([(channel, []) for channel in channels])
        for channel, image_id in self.cursor:
            result[channel].append(image_id)
        return result
        
    def get_imageset_channels(self, name):
        '''Get the channels defined on the imageset
        
        name - the name of the imageset
        '''
        self.cursor.execute("""
        select ic.name from imageset i 
          join channel ic on i.id = ic.imageset_id
         where i.name = ? order by ic.name""", [ name ])
        return [x[0] for x in self.cursor]
    
    def get_imageset_keys(self, name):
        '''Get the metadata keys that were used to produce the imageset
        
        If the imageset was created by matching image metadata for
        a set of keys, the keys are stored in the database and each
        row has a unique set of values per key.
        
        name - name of the imageset
        '''
        self.cursor.execute("""
        select mk.key from metadata_key mk
          join imageset_key ik on ik.metadata_key_id = mk.id
          join imageset i on i.id = ik.imageset_id
         where i.name = ?""", [name])
        return [x[0] for x in self.cursor]
    
    def get_imageset_row_metadata(self, name, image_number):
        '''Return the imageset row's metadata values
        
        name - the name of the imageset
        
        image_number - the one-based image number for the row
        
        returns a dictionary of metadata key and value
        '''
        self.cursor.execute("""
        select metadata_key.key, metadata_value.value
          from imageset
          join imageset_row on imageset_row.imageset_id = imageset.id
          join imageset_metadata on imageset_metadata.imageset_row_id = imageset_row.id
          join metadata_value on imageset_metadata.metadata_value_id = metadata_value.id
          join metadata_key on metadata_value.metadata_key_id = metadata_key.id
         where imageset.name = ? and imageset_row.image_number = ?""", [name, image_number])
        return dict([(k, v) for k, v in self.cursor])
        
    def get_problem_imagesets(self, name):
        '''Return imageset rows with missing or duplicate images
        
        name - the name of the imageset
        
        returns a sequence of tuples of image_number, channel name and the
        count of number of images
        '''
        #
        # OK, typically complex.
        # The query groups by row and channel, counting the
        # images per channel. The subquery collects image rows
        # and channels using a semi-cartesian join (channels x rows)
        # and then we outer-join to the imageset_image table. The
        # subquery is needed to make the outer-join on channel and row.
        #
        # Maybe it is clear enough to understand.
        #
        self.cursor.execute("""
        select s.image_number, s.channel_name, count(ii.image_id)
          from (
              select ir.image_number as image_number,
                     ir.id as imageset_row_id,
                     ic.id as channel_id,
                     ic.name as channel_name
                from imageset i
                join imageset_row ir on i.id = ir.imageset_id
                join channel ic on ic.imageset_id = i.id
               where i.name = ?) s
          left outer join imageset_image ii
            on ii.imageset_row_id = s.imageset_row_id
           and ii.channel_id = s.channel_id
         group by s.image_number, s.channel_name
         having count(ii.image_id) != 1
         order by s.image_number, s.channel_name""", [name])
        #
        # Really, was that so bad?
        #
        return [(image_number, channel_name, count)
                for image_number, channel_name, count in self.cursor]
    
    def add_image_to_imageset(self, image_id, name, image_number, channel):
        '''Add a single image to an imageset
        
        This method can be used to patch up an imageset if the user
        wants to manually correct it image by image.
        
        image_id - the image id of the image
        
        name - the name of the imageset
        
        image_number - add the image to the row with this image number
        
        channel - the image's channel
        '''
        self.cursor.execute(
            """insert into imageset_image (imageset_row_id, image_id, channel_id)
            select ir.id as imageset_row_id, ? as image_id, ic.id as channel_id
              from imageset i
              join imageset_row ir on ir.imageset_id = i.id
              join channel ic on ic.imageset_id = i.id
             where i.name = ?
               and ir.image_number = ?
               and ic.name = ?""", [image_id, name, image_number, channel])
    
    def remove_image_from_imageset(self, name, image_id):
        '''Remove an image frame from an imageset
        
        Remove the image with the given image_id from the imageset.
        '''
        self.cursor.execute(
            """delete from imageset_image
                where image_id = ?
                  and exists (select 'x' from imageset i
                                join imageset_row ir on i.id = ir.imageset_id
                               where ir.id = imageset_row_id
                                 and i.name = ?)""", [image_id, name])
        
    def add_channel_to_imageset(self, name, keys, urlset = None,
                                channel_name = None, 
                                channel_key = None, channel_value = None):
        '''Add a channel to an imageset
        
        Add the images in a urlset to create a new channel in an imageset.
        Each image might be applied to one or more imagesets and might be
        applied to none if its metadata values don't match any in the
        imageset.
        The key set should be a subset (x[:] is a subset of x[:]) of that of the 
        imageset. The function will identify all rows in the imageset whose
        metadata values match those of an image in the urlset and will include
        that image in each of the matching rows.
        
        For instance, image correction might be done per-plate and the user has
        a urlset consisting of one image per plate. This function would
        be called with keys = ["Plate"] to assign one image to all rows
        for each plate.
        
        name - the name of the imageset
        
        urlset - the name of the urlset containing the images to be applied
        
        keys - the key names of the unique metadata for the frames
        
        channel_name - the name to be assigned to the channel. If none,
                       the channel_value will be the channel name
                       
        channel_key - the channel metadata key. If None, all frames in the
                      urlset will be assigned to the imageset
                      
        channel_value - the metadata value for the channel, for instance,
                        "w1" for "wavelength"
        '''
        #
        # NOTE: this is currently a join between two subqueries. We
        #       can speed it up by making it into one big select statement.
        #
        # First, add the channel to the channel
        #
        if channel_key is None:
            # Add by name / no metadata
            assert channel_value is None, "You must specify a key/value pair for the channel"
            assert urlset is not None, "You must use a URL set to choose the channel's images when creating a channel by name"
            self.cursor.execute("""
            insert into channel (imageset_id, name)
            select i.id as imageset_id, ? as name
              from imageset i where i.name = ?""", [channel_name, name])
        elif channel_name is None:
            # Add by metadata key / value
            self.cursor.execute("""
            insert into channel (imageset_id, channel_id, name)
            select i.id as imageset_id, mv.id as channel_id, mv.value as name
              from imageset i
              join metadata_value mv
              join metadata_key mk on mv.metadata_key_id = mk.id
             where i.name = ? and mk.key = ? and mv.value = ?""", 
                [name, channel_key, channel_value])
        else:
            # Add by name, but fill in the channel_id
            self.cursor.execute("""
            insert into channel (imageset_id, metadata_value_id, name)
            select imageset.id as imageset_id, metadata_value.id as metadata_value_id, ? as name
              from imageset
              join metadata_value
              join metadata_key on metadata_value.metadata_key_id = metadata_key.id
             where imageset.name = ? and metadata_key.key = ? and metadata_value.value = ?""", 
                [channel_name, name, channel_key, channel_value])
        channel_id = self.cursor.lastrowid
        #
        # Make a subquery that picks the images.
        #
        fields = "image.id as image_id"
        join = "image "
        where = "1=1" # stupid, but we can assume that where += " and .." works
        params = []
        row_metadata_aliases = ["m%d" % (i+1) for i, key in enumerate(keys)]
        row_key_aliases = ["mk%d" % (i+1) for i, key in enumerate(keys)]
        row_value_aliases = ["mv%d" % (i+1) for i, key in enumerate(keys)]
        for i, key in enumerate(keys):
            fields += ", %s.id as mv%d_id" % (row_value_aliases[i], i+1)
            join += " join metadata %s on image.id = %s.image_id" % (
                row_metadata_aliases[i], row_metadata_aliases[i])
            join += (" join metadata_value %s on %s.id = %s.metadata_value_id" %
                     (row_value_aliases[i], row_value_aliases[i], 
                      row_metadata_aliases[i]))
            join += (" join metadata_key %s on %s.id = %s.metadata_key_id" %
                     (row_key_aliases[i], row_key_aliases[i], 
                      row_value_aliases[i]))
            where += " and %s.key = ?" % row_key_aliases[i]
            params.append(key)
        #
        # Add the urlset join if present
        #
        if urlset is not None:
            join += " join urlset_image on urlset_image.image_id = image.id"
            join += " join urlset on urlset_image.urlset_id = urlset.id"
            where += " and urlset.name = ?"
            params.append(urlset)
        #
        # if the channel key and value are given, add a join from the image
        # to the id for the channel metadata value
        #
        if channel_key is not None:
            join += " join metadata on metadata.image_id = image.id"
            join += " join metadata_value on metadata.metadata_value_id = metadata_value.id"
            join += " join metadata_key on metadata_value.metadata_key_id = metadata_key.id"
            where += " and metadata_key.key = ? and metadata_value.value = ?"
            params += [channel_key, channel_value]
        image_subquery = "select %s from %s where %s" % (fields, join, where)
        image_subquery_params = params
        #
        # Now, we need a subquery where each row has the image number
        # and the metadata values for that imageset row
        #
        fields = "imageset_row.id as imageset_row_id"
        join = "imageset join imageset_row on imageset_row.imageset_id = imageset.id"
        where = "1=1"
        params = []
        for i, key in enumerate(keys):
            n = i+1
            fields += ", im%(n)d.metadata_value_id as mv%(n)d_id" % locals()
            join += " join imageset_metadata im%(n)d on im%(n)d.imageset_row_id = imageset_row.id" % locals()
            join += " join metadata_value mv%(n)d on mv%(n)d.id = im%(n)d.metadata_value_id" % locals()
            join += " join metadata_key mk%(n)d on mk%(n)d.id = mv%(n)d.metadata_key_id" % locals()
            where += " and mk%(n)d.key = ?" % locals()
            params.append(key)
        set_subquery = "select %s from %s where %s" % (fields, join, where)
        set_params = params
        #
        # And finally, the join of the two.
        #
        condition = " and ".join(["si.mv%d_id=sset.mv%d_id" % (i+1,i+1)
                                  for i,key in enumerate(keys)])
        statement = """
        insert into imageset_image (imageset_row_id, image_id, channel_id)
        select sset.imageset_row_id, si.image_id, ? as channel_id
        from (%s) si
        join (%s) sset on %s""" % (image_subquery, set_subquery, condition)
        params = [channel_id] + image_subquery_params + set_params
        #
        # Spinny beach ball
        #
        self.cursor.execute(statement, params)
