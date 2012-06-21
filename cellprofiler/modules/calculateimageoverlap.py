'''<b>Calculate Image Overlap </b> calculates how much overlap occurs between the white portions of two black and white images
<hr>

This module calculates overlap by determining precision, recall, F-factor, false positive rate, and false 
negative rate.  One image is considered the "ground truth" (possibly the result of hand-segmentation) and the other
is the "test image"; the images are determined to overlap most completely when the test image matches the ground
truth perfectly.  The module requires binary (black and white) input, where the objects that have been segmented 
are white and the background is black.  If you segment your images in CellProfiler using <b>IdentifyPrimaryObjects</b>, 
you can create such an image using <b>ConvertObjectsToImage</b> by selecting <i>Binary</i> as the color type.  

If your images have been segmented using other image processing software, or you have hand-segmented them in software 
such as Photoshop, you may need to use one or more of the following to prepare the images for this module:
<ul>
<li> <b>ImageMath</b>: If the objects are black and the background is white, you must invert the intensity using this module.</li>
<li> <b>ApplyThreshold</b>: If the image is grayscale, you must make it binary using this module, or alternately use an <b>Identify</b> module followed by <b>ConvertObjectsToImage</b> as described above. </li>
<li> <b>ColorToGray</b>: If the image is in color, you must first convert it to grayscale using this module, and then use <b>ApplyThreshold</b> to generate a binary image. </li>
</ul>

In the test image, any foreground (white) pixels that overlap with the foreground of the ground
truth will be considered "true positives", since they are correctly labeled as foreground.  Background (black) 
pixels that overlap with the background of the ground truth image are considered "true negatives", 
since they are correctly labeled as background.  A foreground pixel in the test image that overlaps with the background in the ground truth image will
be considered a "false positive" (since it should have been labeled as part of the background), 
while a background pixel in the test image that overlaps with foreground in the ground truth will be considered a "false negative"
(since it was labeled as part of the background, but should not be).

<h4>Available measurements</h4>
<ul>
<li><i>False positive rate:</i> Total number of false positive pixels / total number of actual negative pixels </li>
<li><i>False negative rate:</i> Total number of false negative pixels / total number of actual postive pixels </li>
<li><i>Precision:</i> Number of true positive pixels / (number of true positive pixels + number of false positive pixels) </li>
<li><i>Recall:</i> Number of true positive pixels/ (number of true positive pixels + number of false negative pixels) </li>
<li><i>F-factor:</i> 2 x (precision x recall)/(precision + recall). Also known as F<sub>1</sub> score, F-score or F-measure.</li>
</ul>

'''
# CellProfiler is distributed under the GNU General Public License.
# See the accompanying file LICENSE for details.
# 
# Copyright (c) 2003-2009 Massachusetts Institute of Technology
# Copyright (c) 2009-2012 Broad Institute
# 
# Please see the AUTHORS file for credits.
# 
# Website: http://www.cellprofiler.org

__version__="$Revision: 9000 $"

import numpy as np
from contrib.english import ordinal

from scipy.ndimage import label
from scipy.sparse import coo_matrix

import cellprofiler.cpimage as cpi
import cellprofiler.cpmodule as cpm
import cellprofiler.measurements as cpmeas
import cellprofiler.settings as cps

C_IMAGE_OVERLAP = "Overlap"
FTR_F_FACTOR = "Ffactor"
FTR_PRECISION = "Precision"
FTR_RECALL = "Recall"
FTR_FALSE_POS_RATE = "FalsePosRate"
FTR_FALSE_NEG_RATE = "FalseNegRate"
FTR_RAND_INDEX = "RandIndex"
FTR_ADJUSTED_RAND_INDEX = "AdjustedRandIndex"

FTR_ALL = [FTR_F_FACTOR, FTR_PRECISION, FTR_RECALL,
           FTR_FALSE_POS_RATE, FTR_FALSE_NEG_RATE,
           FTR_RAND_INDEX, FTR_ADJUSTED_RAND_INDEX]

O_OBJ = "Segmented objects"
O_IMG = "Foreground/background segmentation"
O_ALL = [O_OBJ, O_IMG]

L_LOAD = "Loaded from a previous run"
L_CP = "From this CP pipeline"

class CalculateImageOverlap(cpm.CPModule):
    
    category = "Image Processing"
    variable_revision_number = 2
    module_name = "CalculateImageOverlap"

    def create_settings(self):
        self.obj_or_img = cps.Choice("Compare segmented objects, or foreground/background?", O_ALL)
        self.ground_truth = cps.ImageNameSubscriber("Select the image to be used as the ground truth basis for calculating the amount of overlap", "None", doc = 
                                                    '''This binary (black and white) image is known as the "ground truth" image.  It can be the product of segmentation performed by hand, or
                                                   the result of another segmentation algorithm whose results you would like to compare.''')
        self.test_img = cps.ImageNameSubscriber("Select the image to be used to test for overlap", "None", doc = ''' This 
                                                binary (black and white) image is what you will compare with the ground truth image. It is known as the "test image".''')
        self.object_name_GT = cps.ObjectNameSubscriber("Select the objects to be used as the ground truth basis for calculating the amount of overlap", "None")
        self.img_obj_found_in_GT = cps.ImageNameSubscriber("Which image did you find these objects in?","None")
        self.object_name_ID = cps.ObjectNameSubscriber("Select the objects to be tested for overlap against the ground truth", "None")
        self.img_obj_found_in_ID = cps.ImageNameSubscriber("Which image did you find these objects in?","None")


    def settings(self):
        result = [self.obj_or_img, self.ground_truth, self.test_img, self.object_name_GT, self.img_obj_found_in_GT,self.object_name_ID, self.img_obj_found_in_ID]
        return result

    def visible_settings(self):
        result = [self.obj_or_img]
        if self.obj_or_img == O_IMG:
            result += [self.ground_truth, self.test_img]
        elif self.obj_or_img == O_OBJ:
            result += [self.object_name_GT, self.img_obj_found_in_GT,self.object_name_ID, self.img_obj_found_in_ID]
        return result

    def run(self,workspace):
        if self.obj_or_img == O_IMG:
            self.measure_image(workspace)
        elif self.obj_or_img == O_OBJ:
            self.measure_objects(workspace)

    def measure_image(self, workspace):
        '''Add the image overlap measurements'''
        
        image_set = workspace.image_set
        ground_truth_image = image_set.get_image(self.ground_truth.value,
                                                 must_be_binary = True)
        test_image = image_set.get_image(self.test_img.value,
                                         must_be_binary = True)
        ground_truth_pixels = ground_truth_image.pixel_data
        ground_truth_pixels = test_image.crop_image_similarly(ground_truth_pixels)
        mask = ground_truth_image.mask
        mask = test_image.crop_image_similarly(mask)
        if test_image.has_mask:
            mask = mask & test_image.mask
        test_pixels = test_image.pixel_data
        
        false_positives = test_pixels & ~ ground_truth_pixels
        false_positives[~ mask] = False
        false_negatives = (~ test_pixels) & ground_truth_pixels
        false_negatives[~ mask] = False
        true_positives = test_pixels & ground_truth_pixels
        true_positives[ ~ mask] = False
        true_negatives = (~ test_pixels) & (~ ground_truth_pixels)
        true_negatives[~ mask] = False
        
        false_positive_count = np.sum(false_positives)
        true_positive_count = np.sum(true_positives)
        
        false_negative_count = np.sum(false_negatives)
        true_negative_count = np.sum(true_negatives)
        
        labeled_pixel_count = true_positive_count + false_positive_count
        true_count = true_positive_count + false_negative_count
        
        ##################################
        #
        # Calculate the F-Factor
        #
        # 2 * precision * recall
        # -----------------------
        # precision + recall
        #
        # precision = true positives / labeled
        # recall = true positives / true count
        #
        ###################################
        
        if labeled_pixel_count == 0:
            precision = 1.0
        else:
            precision = float(true_positive_count) / float(labeled_pixel_count)
        if true_count == 0:
            recall = 1.0
        else:
            recall = float(true_positive_count) / float(true_count)
        if (precision + recall) == 0:
            f_factor = 0.0 # From http://en.wikipedia.org/wiki/F1_score
        else:
            f_factor = 2.0 * precision * recall / (precision + recall)
        negative_count = false_positive_count + true_negative_count
        if negative_count == 0:
            false_positive_rate = 0.0
        else:
            false_positive_rate = (float(false_positive_count) / 
                                   float(negative_count))
        if true_count == 0:
            false_negative_rate = 0.0
        else:
            false_negative_rate = (float(false_negative_count) / 
                                   float(true_count))
        ###############################
        #
        # Caluclate the Rand Index
        # http://en.wikipedia.org/wiki/Rand_index
        #
        # Given a set of N elements and two partitions of that set, X and Y
        #
        # A = the number of pairs of elements in S that are in the same set in
        #     X and in the same set in Y
        # B = the number of pairs of elements in S that are in different sets
        #     in X and different sets in Y
        # C = the number of pairs of elements in S that are in the same set in
        #     X and different sets in Y
        # D = the number of pairs of elements in S that are in different sets
        #     in X and the same set in Y
        #
        # The rand index is:   A + B
        #                      -----
        #                      A+B+C+D
        ##########################
        
        ground_truth_labels, ground_truth_count = label(
            ground_truth_pixels & mask, np.ones((3, 3), bool))
        test_labels, test_count = label(
            test_pixels & mask, np.ones((3, 3), bool))
        ground_truth_labels = ground_truth_labels[mask].astype(np.uint64)
        test_labels = test_labels[mask].astype(np.uint64)
        if len(test_labels) > 0:
            #
            # Create a sparse matrix of the pixel labels in each of the sets
            # 
            # The matrix, N(i,j) gives the counts of all of the pixels that were
            # labeled with label I in the ground truth and label J in the 
            # test set.
            #
            N_ij = coo_matrix((np.ones(len(test_labels)), 
                               (ground_truth_labels, test_labels))).toarray()
            def choose2(x):
                '''Compute # of pairs of x things = x * (x-1) / 2'''
                return x * (x - 1) / 2
            #
            # Each cell in the matrix is a count of a grouping of pixels whose
            # pixel pairs are in the same set in both groups. The number of 
            # pixel pairs is n * (n - 1), so A = sum(matrix * (matrix - 1))
            #
            A = np.sum(choose2(N_ij))
            #
            # B is the sum of pixels that were classified differently by both
            # sets. But the easier calculation is to find A, C and D and get
            # B by subtracting A, C and D from the N * (N - 1), the total
            # number of pairs.
            #
            # For C, we take the number of pixels classified as "i" and for each
            # "j", subtract N(i,j) from N(i) to get the number of pixels in
            # N(i,j) that are in some other set = (N(i) - N(i,j)) * N(i,j)
            #
            # We do the similar calculation for D
            #
            N_i = np.sum(N_ij, 1)
            N_j = np.sum(N_ij, 0)
            C = np.sum((N_i[:, np.newaxis] - N_ij) * N_ij) / 2
            D = np.sum((N_j[np.newaxis, :] - N_ij) * N_ij) / 2
            total = choose2(len(test_labels))
            # an astute observer would say, why bother computing A and B
            # when all we need is A+B and C, D and the total can be used to do
            # that. The calculations aren't too expensive, though, so I do them.
            B = total - A - C - D
            rand_index = (A + B) / total
            #
            # The adjusted rand index is the rand index adjusted for chance
            # so as not to penalize situations with many segmentations.
            #
            # Jorge M. Santos, Mark Embrechts, "On the Use of the Adjusted Rand 
            # Index as a Metric for Evaluating Supervised Classification",
            # Lecture Notes in Computer Science, 
            # Springer, Vol. 5769, pp. 175-184, 2009. Eqn # 6
            #
            # ExpectedIndex = best possible score
            #
            # ExpectedIndex = sum(N_i choose 2) * sum(N_j choose 2) 
            #
            # MaxIndex = worst possible score = 1/2 (sum(N_i choose 2) + sum(N_j choose 2)) * total
            #
            #  A * total - ExpectedIndex
            # -------------------------
            #  MaxIndex - ExpectedIndex
            #
            # 
            expected_index = np.sum(choose2(N_i)) * np.sum(choose2(N_j))
            max_index = (np.sum(choose2(N_i)) + np.sum(choose2(N_j))) * total / 2
            
            adjusted_rand_index = \
                (A * total - expected_index) / (max_index - expected_index)
        else:
            rand_index = adjusted_rand_index = np.nan
            
        m = workspace.measurements
        m.add_image_measurement(self.measurement_name(FTR_F_FACTOR), f_factor)
        m.add_image_measurement(self.measurement_name(FTR_PRECISION),
                                precision)
        m.add_image_measurement(self.measurement_name(FTR_RECALL), recall)
        m.add_image_measurement(self.measurement_name(FTR_FALSE_POS_RATE),
                                false_positive_rate)
        m.add_image_measurement(self.measurement_name(FTR_FALSE_NEG_RATE),
                                false_negative_rate)
        m.add_image_measurement(self.measurement_name(FTR_RAND_INDEX),
                                rand_index)
        m.add_image_measurement(self.measurement_name(FTR_ADJUSTED_RAND_INDEX),
                                adjusted_rand_index)
        
        if self.show_window:
            workspace.display_data.true_positives = true_positives
            workspace.display_data.true_negatives = true_negatives
            workspace.display_data.false_positives = false_positives
            workspace.display_data.false_negatives = false_negatives
            workspace.display_data.rand_index = rand_index
            workspace.display_data.adjusted_rand_index = adjusted_rand_index
            workspace.display_data.statistics = [
                ("Measurement", "Value"),
                (FTR_F_FACTOR, f_factor),
                (FTR_PRECISION, precision),
                (FTR_RECALL, recall),
                (FTR_FALSE_POS_RATE, false_positive_rate),
                (FTR_FALSE_NEG_RATE, false_negative_rate),
                (FTR_RAND_INDEX, rand_index),
                (FTR_ADJUSTED_RAND_INDEX, adjusted_rand_index)
            ]
            
    def measure_objects(self, workspace):
        image_set = workspace.image_set
        GT_img = image_set.get_image(self.img_obj_found_in_GT.value)
        ID_img = image_set.get_image(self.img_obj_found_in_ID.value)
        ID_pixels = ID_img.pixel_data
        GT_pixels = GT_img.pixel_data
        object_name_GT = self.object_name_GT.value
        objects_GT = workspace.get_objects(object_name_GT)
        iGT,jGT,lGT = objects_GT.ijv.transpose() 
        object_name_ID = self.object_name_ID.value
        objects_ID = workspace.get_objects(object_name_ID)
        iID, jID, lID = objects_ID.ijv.transpose()
        ID_obj = max(lID)
        GT_obj  = max(lGT)
        intersect_matrix = np.zeros((ID_obj, GT_obj))
        GT_tot_area = []
        all_intersect_area = []
        FN_area = np.zeros((ID_obj, GT_obj))

        xGT, yGT = np.shape(GT_pixels)
        xID, yID = np.shape(ID_pixels)
        GT_pixels = np.zeros((xGT, yGT))
        ID_pixels = np.zeros((xID, yID))
        total_pixels = xGT*yGT

        for ii in range(0, GT_obj):
            indices_ii = np.nonzero(lGT == ii)
            indices_ii = indices_ii[0]
            iGT_ii = iGT[indices_ii]
            jGT_ii = jGT[indices_ii]
            GT_set = set(zip(iGT_ii, jGT_ii))
            for jj in range(0, ID_obj):
                indices_jj = np.nonzero(lID==jj)
                indices_jj = indices_jj[0]
                iID_jj = iID[indices_jj]
                jID_jj = jID[indices_jj]
                ID_set = set(zip(iID_jj, jID_jj))
                area_overlap = len(GT_set & ID_set)
                all_intersect_area += [area_overlap]
                intersect_matrix[jj,ii] = area_overlap
                FN_area[jj,ii] = len(GT_set) - area_overlap
            GT_pixels[iGT, jGT] = 1    
            GT_tot_area += [len(GT_set)]

        dom_ID = []

        for i in range(0, ID_obj):
            indices_jj = np.nonzero(lID==i)
            indices_jj = indices_jj[0]
            id_i = iGT[indices_jj]
            id_j = jGT[indices_jj]
            ID_pixels[id_i, id_j] = 1

        for i in intersect_matrix:  # loop through the GT objects first                                
            if max(i) == 0:
                id = -1  # we missed the object; arbitrarily assign -1 index                                                          
            else:
                id = np.where(i == max(i))[0][0] # what is the ID of the max pixels?                                                            
            dom_ID += [id]  # for ea GT object, which is the dominating ID?                                                                    

        dom_ID = np.array(dom_ID)
        
        for i in range(0, len(intersect_matrix.T)):
            if len(np.where(dom_ID == i)[0]) > 1:
                final_id = np.where(intersect_matrix.T[i] == max(intersect_matrix.T[i]))
                final_id = final_id[0][0]
                all_id = np.where(dom_ID == i)[0]
                nonfinal = [x for x in all_id if x != final_id]
                for n in nonfinal:  # these others cannot be candidates for the corr ID now                                                      
                    intersect_matrix.T[i][n] = 0
            else :
                continue

        TP = []
        TN = []
        FN = []
        for i in range(0,len(dom_ID)):
            d = dom_ID[i]
            tp = intersect_matrix[i][d]
            TP += [tp]
            tp = intersect_matrix[i][d]
            fn = FN_area[i][d]
            tn = total_pixels - tp
            TP += [tp]
            TN += [tn]
            FN += [fn]

        FP = []
        for i in range(0,len(dom_ID)):
            d = dom_ID[i]
            fp = np.sum(intersect_matrix[i][0:d])+np.sum(intersect_matrix[i][(d+1)::])
            FP += [fp]
            d = dom_ID[i]
   
        FN = np.sum(FN)
        TN = np.sum(TN)
        TP = np.sum(TP)
        FP = np.sum(FP)
        GT_tot_area = np.sum(GT_tot_area)

        all_intersecting_area = np.sum(all_intersect_area)

        
        accuracy = TP/all_intersecting_area
        recall  = TP/GT_tot_area
        precision = TP/(TP+FP)
        F_factor = 2*(precision*recall)/(precision+recall)
        false_positive_rate = FP/(FP+TN)
        false_negative_rate = FN/(FN+TP)

        m = workspace.measurements
        m.add_image_measurement(self.measurement_name(FTR_F_FACTOR), F_factor)
        m.add_image_measurement(self.measurement_name(FTR_PRECISION),
                                precision)
        m.add_image_measurement(self.measurement_name(FTR_RECALL), recall)
        m.add_image_measurement(self.measurement_name(FTR_FALSE_POS_RATE),
                                false_positive_rate)
        m.add_image_measurement(self.measurement_name(FTR_FALSE_NEG_RATE),
                                false_negative_rate)
        def subscripts(condition1, condition2):
            x1,y1 = np.where(GT_pixels == condition1)
            x2,y2 = np.where(ID_pixels == condition2)
            mask = set(zip(x1,y1)) & set(zip(x2,y2))
            return list(mask)

        TP_mask = subscripts(1,1)
        FN_mask = subscripts(1,0)
        FP_mask = subscripts(0,1)
        TN_mask = subscripts(0,0)

        TP_pixels = np.zeros((xGT,yGT))
        FN_pixels = np.zeros((xGT,yGT))
        FP_pixels = np.zeros((xGT,yGT))
        TN_pixels = np.zeros((xGT,yGT))

        def maskimg(mask,img):
            for ea in mask:
                img[ea] = 1
            return img

        TP_pixels = maskimg(TP_mask, TP_pixels)
        FN_pixels = maskimg(FN_mask, FN_pixels)
        FP_pixels = maskimg(FP_mask, FP_pixels)
        TN_pixels = maskimg(TN_mask, TN_pixels)

        if self.show_window:
            workspace.display_data.true_positives = TP_pixels
            workspace.display_data.true_negatives = FN_pixels
            workspace.display_data.false_positives = FP_pixels
            workspace.display_data.false_negatives = TN_pixels
            workspace.display_data.statistics = [
                ("Measurement", "Value"),
                (FTR_F_FACTOR, F_factor),
                (FTR_PRECISION, precision),
                (FTR_RECALL, recall),
                (FTR_FALSE_POS_RATE, false_positive_rate),
                (FTR_FALSE_NEG_RATE, false_negative_rate)]



    def display(self, workspace, figure):
        '''Display the image confusion matrix & statistics'''
        figure.set_subplots((2, 3))
        for x, y, image, label in (
            (0, 0, workspace.display_data.true_positives, "True positives"),
            (0, 1, workspace.display_data.false_positives, "False positives"),
            (1, 0, workspace.display_data.false_negatives, "False negatives"),
            (1, 1, workspace.display_data.true_negatives, "True negatives")):
            figure.subplot_imshow_bw(x, y, image, title=label,
                                     sharex=figure.subplot(0,0),
                                     sharey=figure.subplot(0,0))
            
        figure.subplot_table(1, 2, workspace.display_data.statistics,
                             ratio = (.5, .5))

    def measurement_name(self, feature):
        if self.obj_or_img == O_IMG:
            name = '_'.join((C_IMAGE_OVERLAP, feature, self.test_img.value))
        if self.obj_or_img == O_OBJ:
            name = '_'.join((C_IMAGE_OVERLAP, feature, self.img_obj_found_in_GT.value))
        return name 


    
    def get_categories(self, pipeline, object_name):
        '''Return the measurement categories for an object'''
        if object_name == cpmeas.IMAGE:
            return [ C_IMAGE_OVERLAP ]
        return []
    
    def get_measurements(self, pipeline, object_name, category):
        '''Return the measurements made for a category'''
        if object_name == cpmeas.IMAGE and category == C_IMAGE_OVERLAP:
            return FTR_ALL
        return []
    
    def get_measurement_images(self, pipeline, object_name, category, 
                               measurement):
        '''Return the images that were used when making the measurement'''
        if (object_name == cpmeas.IMAGE and category == C_IMAGE_OVERLAP and
            measurement in FTR_ALL):
            return [self.test_img.value]
        return []
    
    def get_measurement_columns(self, pipeline):
        '''Return database column information for each measurement'''
        return [ (cpmeas.IMAGE,
                  '_'.join((C_IMAGE_OVERLAP, feature, self.test_img.value)),
                  cpmeas.COLTYPE_FLOAT)
                 for feature in FTR_ALL]

    def upgrade_settings(self, setting_values, variable_revision_number, 
                         module_name, from_matlab):
        if from_matlab:
            # Variable revision # wasn't in Matlab file
            # All settings were identical to CP 2.0 v 1
            from_matlab = False
            variable_revision_number = 1
        if variable_revision_number == 1:
            #no object choice before rev 2
            old_setting_values = setting_values
            setting_values = [O_IMG, old_setting_values[0], old_setting_values[1]]
            variable_revision_number = 2
        return setting_values, variable_revision_number, from_matlab
