function handles = GetDataOnImage(handles)

% Help for the Get Histogram module:
% Category: Other
%
% Feature Number:
% The feature number is the parameter from the chosen module (AreaShape,
% Intensity, Texture) which will be used for displaying the data. The
% following tables provide the feature numbers for each measurement made by
% the three modules:
%
% Area Shape:               Feature Number:
% Area                    |       1
% Eccentricity            |       2
% Solidity                |       3
% Extent                  |       4
% Euler Number            |       5
% Perimeter               |       6
% Form factor             |       7
% MajorAxisLength         |       8
% MinorAxisLength         |       9
%
% Intensity:                Feature Number:
% IntegratedIntensity     |       1
% MeanIntensity           |       2
% StdIntensity            |       3
% MinIntensity            |       4
% MaxIntensity            |       5
% IntegratedIntensityEdge |       6
% MeanIntensityEdge       |       7
% StdIntensityEdge        |       8
% MinIntensityEdge        |       9
% MaxIntensityEdge        |      10
% MassDisplacement        |      11
%
% Texture:                  Feature Number:
% AngularSecondMoment     |       1
% Contrast                |       2
% Correlation             |       3
% Variance                |       4
% InverseDifferenceMoment |       5
% SumAverage              |       6
% SumVariance             |       7
% SumEntropy              |       8
% Entropy                 |       9
% DifferenceVariance      |      10
% DifferenceEntropy       |      11
% InformationMeasure      |      12
% InformationMeasure2     |      13
% Gabor1x                 |      14
% Gabor1y                 |      15

% CellProfiler is distributed under the GNU General Public License.
% See the accompanying file LICENSE for details.
%
% Developed by the Whitehead Institute for Biomedical Research.
% Copyright 2003,2004,2005.
%
% Authors:
%   Anne Carpenter
%   Thouis Jones
%   In Han Kang
%   Ola Friman
%   Steve Lowe
%   Joo Han Chang
%   Colin Clarke
%   Mike Lamprecht
%   Susan Ma
%   Wyman Li
%
% Website: http://www.cellprofiler.org
%
% $Revision: 2614 $

%%% Reads the current module number, because this is needed to find
%%% the variable values that the user entered.

CurrentModule = handles.Current.CurrentModuleNumber;
CurrentModuleNum = str2double(CurrentModule);
ModuleName = char(handles.Settings.ModuleNames(CurrentModuleNum));

%textVAR01 = Which object would you like to use for the data (The option IMAGE currently only works with Correlation measurements)?
%choiceVAR01 = Image
%infotypeVAR01 = objectgroup
%inputtypeVAR01 = popupmenu
ObjectName = char(handles.Settings.VariableValues{CurrentModuleNum,1});

%textVAR02 = Which category of measurements would you like to use?
%choiceVAR02 = AreaShape
%choiceVAR02 = Correlation
%choiceVAR02 = Intensity
%choiceVAR02 = Neighbors
%choiceVAR02 = Texture
%inputtypeVAR02 = popupmenu custom
Measure = char(handles.Settings.VariableValues{CurrentModuleNum,2});

%textVAR03 = Which feature do you want to use? (Enter the feature number - see HELP for explanation)
%defaultVAR03 = 1
FeatureNo = str2double(handles.Settings.VariableValues{CurrentModuleNum,3});

if isempty(FeatureNo)
    error('You entered an incorrect Feature Number.');
end

%textVAR04 = If using INTENSITY or TEXTURE measures, which image was used to make the measurements?
%infotypeVAR04 = imagegroup
%inputtypeVAR04 = popupmenu
Image = char(handles.Settings.VariableValues{CurrentModuleNum,4});

%textVAR05 = Which image do you want to display the data on?
%infotypeVAR05 = imagegroup
%inputtypeVAR05 = popupmenu
DisplayImage = char(handles.Settings.VariableValues{CurrentModuleNum,5});

%textVAR06 = What do you want to call the generated image with data?
%defaultVAR06 = OrigHist
%infotypeVAR06 = imagegroup indep
DataImage = char(handles.Settings.VariableValues{CurrentModuleNum,6});

%%%VariableRevisionNumber = 1

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% PRELIMINARY CALCULATIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
drawnow

%%% Determines which image set is being analyzed.
SetBeingAnalyzed = handles.Current.SetBeingAnalyzed;

if strcmp(Measure,'Intensity') || strcmp(Measure,'Texture')
    Measure = [Measure, '_',Image];
end

%%% Checks whether the image to be analyzed exists in the handles structure.
if ~isfield(handles.Pipeline, DisplayImage)
    %%% If the image is not there, an error message is produced.  The error
    %%% is not displayed: The error function halts the current function and
    %%% returns control to the calling function (the analyze all images
    %%% button callback.)  That callback recognizes that an error was
    %%% produced because of its try/catch loop and breaks out of the image
    %%% analysis loop without attempting further modules.
    error(['Image processing was canceled in the ', ModuleName, ' module because it could not find the input image.  It was supposed to be named ', ImageName, ' but an image with that name does not exist.  Perhaps there is a typo in the name.'])
end
%%% Reads the image.
OrigImage = handles.Pipeline.(DisplayImage);
if max(OrigImage(:)) > 1 || min(OrigImage(:)) < 0
    CPwarndlg('The images you have loaded are outside the 0-1 range, and you may be losing data.','Outside 0-1 Range','replace');
end

%%%%%%%%%%%%%%%%%%%%%
%%% DATA ANALYSIS %%%
%%%%%%%%%%%%%%%%%%%%%
drawnow

try
    tmp = handles.Measurements.(ObjectName).(Measure){SetBeingAnalyzed};
catch
    error(['The measurements for ',ModuleName,' could not be found. This module must be after a measure module or no objects were identified.']);
end
ListOfMeasurements = tmp(:,FeatureNo);
StringListOfMeasurements = cellstr(num2str(ListOfMeasurements));

%%% Extracts the XY locations. This is temporarily hard-coded
Xlocations = handles.Measurements.(ObjectName).Location{SetBeingAnalyzed}(:,1);
Ylocations = handles.Measurements.(ObjectName).Location{SetBeingAnalyzed}(:,2);

%%%%%%%%%%%%%%%
%%% DISPLAY %%%
%%%%%%%%%%%%%%%
drawnow

fieldname = ['FigureNumberForModule',CurrentModule];
ThisModuleFigureNumber = handles.Current.(fieldname);
%%% Creates the display window.
DataHandle = CPfigure(handles,ThisModuleFigureNumber);
imagesc(OrigImage);
colormap(gray);
FeatureDisp = handles.Measurements.(ObjectName).([Measure,'Features']){FeatureNo};
title([ObjectName,', ',FeatureDisp,' on ',Image])

%%% Overlays the values in the proper location in the image.
TextHandles = text(Xlocations , Ylocations , StringListOfMeasurements,...
    'HorizontalAlignment','center', 'color', [1 1 0],'fontsize',handles.Preferences.FontSize);

%%% Create structure and save it to the UserData property of the window
Info = get(DataHandle,'UserData');
Info.ListOfMeasurements = ListOfMeasurements;
Info.TextHandles = TextHandles;
set(DataHandle,'UserData',Info);

OneFrame = getframe(DataHandle);
handles.Pipeline.(DataImage)=OneFrame.cdata;