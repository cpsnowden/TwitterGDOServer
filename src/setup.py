from distutils.core import setup

setup(
        name='TwitterGDOServer',
        version='',
        packages=['', 'api', 'api.Junk', 'api.Utils', 'api.Objects', 'api.Resources', 'Database', 'DataService',
                  'DataService.TwitterService', 'AnalyticsService', 'AnalyticsService.Graphing',
                  'AnalyticsService.Graphing.Junk'],
        package_dir={'': 'src'},
        url='',
        license='',
        author='ChrisSnowden',
        author_email='',
        description=''
)
