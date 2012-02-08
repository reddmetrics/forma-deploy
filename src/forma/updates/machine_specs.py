import platform, getpass

def getPaths():

    host_os = platform.system()
    user = getpass.getuser()

    if host_os == 'Windows' and user == 'Robin Kraft':
        paths = dict(scripts = 'Z:/robin On My Mac/Documents/workspace/scripts/cloud',
                    temp = 'Z:/robin On My Mac/Desktop/delete/temp/',
                    data_root = 'Z:/robin On My Mac/data/mnt/',
                    s3cmd = 'Z:/robin On My Mac/bin/s3cmd-1.0.0/s3cmd',
                    arc = 'C:/Program Files (x86)/ArcGIS/',
                    stata = 'C:/Program Files (x86)/Stata11/StataSE-64',
                    s3cfg = 'C:/Users/Administrator/Application Data/s3cmd.ini')
        return paths

    elif host_os == 'Windows' and user == 'Administrator':
        paths = dict(scripts = 'C:/Users/Administrator/Documents/Downloads/cloud/',
                    temp = 'C:/Users/Administrator/Documents/Downloads/',
                    data_root = 'C:/Users/Administrator/Desktop/scratch/mnt/',
                    s3cmd='C:/Users/Administrator/Documents/Downloads/s3cmd-1.0.0/s3cmd',
                    arc = 'C:/Program Files/ArcGIS/',
                    stata = 'C:/Users/Administrator/Desktop/Stata11/StataSE.exe',
                    s3cfg = 'C:/Users/Administrator/Application Data/s3cmd.ini')
        return paths

    elif host_os == 'Windows' and user == 'Dan':
        paths = dict(scripts = 'C://Users//Dan//Documents//My Dropbox//n_drive//Scripts//cloud//',
                    temp = 'C://Users//Dan//Desktop//scratch//',
                    data_root = 'C://Users//Dan//Documents//se_asia//',
                    arc = 'C://Program Files (x86)//ArcGIS//',
                    stata = 'C://Program Files (x86)//Stata11//StataSE-64.exe')
                    #s3cfg = 'C:/Users/Administrator/Application Data/s3cmd.ini'
                    # #s3cmd = dir_temp + 's3cmd-0.9.9.91/s3cmd',
        return paths

    elif host_os == 'Darwin' and user == 'danhammer':
        paths = dict(scripts = '/Users/danhammer/Dropbox/n_drive/Scripts/cloud/',
                    temp = '/Users/danhammer/Desktop/workspace/scratch/',
                    data_root = '/Users/danhammer/data/mnt/')
        return paths


    elif host_os == 'Darwin' and user == 'robin':
        paths = dict(scripts = '/Users/robin/Documents/workspace/scripts/cloud/',
                    temp = '/Users/robin/Desktop/delete/temp/',
                    data_root = '/Users/robin/data/mnt/')
        return paths

    elif host_os == 'Linux' and user == 'ubuntu':
        paths = dict(scripts = '/home/%s/' % user,
                    data_root = "/mnt/",
                    temp="/mnt/temp/")
        return paths

    elif host_os == 'Windows' and user == "Robin.Kraft":
        paths = dict(dir_root = 'C:/Users/Administrator/Documents/Downloads/cloud/',
                    dir_temp = 'C:/Users/Administrator/Documents/Downloads/',
                    dir_data_root = 'C:/Users/Administrator/Desktop/scratch/mnt/',
                    s3cmd = 'C:/Users/Administrator/Documents/Downloads/s3cmd-1.0.0/s3cmd',
                    dir_arc = 'C:/Program Files/ArcGIS/',
                    stata = 'C:/Users/Administrator/Desktop/Stata11/StataSE.exe',
                    s3cfg = 'C:/Users/Administrator/Application Data/s3cmd.ini')
        return paths

    else:
        error = 'Username or platform not recognized, so data and script paths are not set up\nUsername: %s\nPlatform: %s\nAdd your details to forma_init.py near line 165' % (user, host_os)
        raise ValueError(error)
