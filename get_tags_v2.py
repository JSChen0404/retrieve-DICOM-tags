import os
import sys
import pickle
import pydicom
import glob
import time
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool 

def retri_tags(in_arg):
    path, date = in_arg
    save_path = str(date)
    
    os.mkdir(save_path, exist_ok=True)
        
    try:
        ds = pydicom.dcmread(path)

        x = [path]
        
        tags = [
            "StudyDescription", "Modality", "SOPInstanceUID", "PatientID", "PatientSex",
            "PatientBirthDate", "StudyDate", "ProtocolName", "BodyPartExamined", "Laterality",
            "Manufacturer", "StationName", "KVP", "DistanceSourceToDetector", "DistanceSourceToPatient",
            "ExposureTime", "Exposure", "XRayTubeCurrent", "ImagerPixelSpacing","InstanceNumber", 
            "SeriesNumber", "StudyInstanceUID", "SeriesInstanceUID", "SeriesDescription"
        ]
        
        # no_use_tags = ["SeriesDate", "SOPClassUID"]
        
        for tag in tags:
            x.append(getattr(ds, tag, ""))
 
        # ProcedureCodeSequence handling
        pp = (ds.ProcedureCodeSequence[0].CodeMeaning if "ProcedureCodeSequence" in ds and
              len(ds.ProcedureCodeSequence) == 1 and 'CodeMeaning' in ds.ProcedureCodeSequence[0] else "")
        x.append(pp)

        with open(os.path.join(save_path, ds.SOPInstanceUID+'.pkl'), 'wb') as file:
            pickle.dump(x, file)
            
        with open(os.path.join(save_path, 'dcm_ok.txt'), 'a') as file:
            file.writelines(path+'\n')
      
    except Exception:
        with open(os.path.join(save_path, 'dcm_ok.txt'), 'a') as file:
            file.writelines(path+'\n')   
    
    return 0


if __name__ == "__main__":
    print("====="*10)
    print("此為取tag程式，注意事項如下：")
    print("====="*10)
    print("1. 請移動至將儲存之位置，再python此程式。")
    print("2. 環境所需package包括pandas與pydicom，若無安裝，請先安裝，指令如下：")
    print("   pip install pydicom")
    print("   pip install pandas\n")
    print("====="*10)
    print("請選擇DICOM檔案路徑的提供方式：")
    print("1. 資料列表 (需含FilePath，內容為DICOM的完整路徑)")
    print("2. 直接輸入DICOM檔案位置")
    print("====="*10)

    num_sel = input("請輸入提供方式代號： ")

    if num_sel == "1":
        csv_file = input("請輸入資料列表的路徑 (.csv): ")
        folder_name = input("請輸入tags檔案儲存資料夾名稱: ")
        try:
            df = pd.read_csv(csv_file)
        except:
            df = pd.read_csv(csv_file, encoding='cp950')
            
        DICOM_files = df['FilePath'].tolist()  

    elif num_sel == "2":
        dcm = input("請輸入Dicom檔案位置: ")
        folder_name = input("請輸入存檔路徑: ")

        DICOM_files = []
        for root, dirs, files in os.walk(dcm):
            for f in files:
                if f.lower().endswith('.dcm'):
                    DICOM_files.append(os.path.join(root, f))


    start_time = time.time()
    print("Start time: " + str(time.ctime(start_time)))
    
    # 資料夾取名為日期與時間，並格式化為指定格式 "2024-08-02 14:20"    
    # folder_name = time.strftime('%Y-%m-%d %H%M', time.localtime(start_time)) 
 
    os.mkdir(folder_name, exist_ok=True)

    try:
        with open(os.path.join(folder_name, 'dcm_ok.txt'), 'r') as file:
            readed_DICOMs = file.read().splitlines()
    except FileNotFoundError:
        readed_DICOMs = []
        
    # 計算未讀取的 DICOM 文件
    dif = list(set(DICOM_files) - set(readed_DICOMs))
    dfs = [[i, folder_name] for i in dif]

    print("Reading OK")
    print("Loading: " + str(len(dfs)))
    
    # 計算需要分割的數量
    num1 = (len(dfs) // 50) * 50

    # p = Pool(1)
    
    # p.map(retri_tags, dfs[num1:])

    # p = Pool(10)
    
    # p.map(retri_tags, dfs[:num1])
    
    # 使用較小的 Pool 來處理尾端部分
    with Pool(1) as pool:
        for _ in tqdm(pool.imap_unordered(retri_tags, dfs[num1:]), total=len(dfs[num1:])):
            pass
    
    # 使用較大的 Pool 來處理主要部分
    with Pool(10) as pool:
        for _ in tqdm(pool.imap_unordered(retri_tags, dfs[:num1]), total=len(dfs[:num1])):
            pass
    
    
    print("\nComplete to extract information~")
    print("End time for extract tags: " + time.ctime())
    
    xx = glob.glob(os.path.join(folder_name, "*.pkl"))
    datas = []
    for num, i in enumerate(xx):
        with open(i, 'rb') as file:
            datas.append(pickle.load(file))
        nn = int(((num+1)/len(xx))*100)
        print("\r", end="")
        print("Loading progress: {}%: ".format(nn), "*" * int(nn // 2), end="")
        sys.stdout.flush()
        time.sleep(0.05)
    
    dfs = pd.DataFrame(datas,
                       columns=['FilePath','StudyDescription', 'Modality','SOPInstanceUID','PatientID','PatientSex',
                                'PatientBirthDate','StudyDate','ProtocolName','BodyPartExamined','Laterality',
                                'Manufacturer','StationName','KVP','DistanceSourceToDetector','DistanceSourceToPatient',
                                'ExposureTime','Exposure','XRayTubeCurrent','ImagerPixelSpacing','InstanceNumber',
                                'SeriesNumber','StudyInstanceUID','SeriesInstanceUID','SeriesDescription','CodeMeaning'])
    dfs.to_csv('tags_' + str(folder_name) + '.csv', index=False, encoding='utf-8-sig')
 
    print("\nComplete time: " + time.ctime())
    elapsed_time = (time.time() - start_time) / 60
    print(f"Overall elapsed time: {elapsed_time:.3f} minutes")
    
